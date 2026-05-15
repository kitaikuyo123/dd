package com.minisql.sql.execution.operators;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.RegionServerServiceGrpc;
import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;
import com.minisql.common.rpc.GrpcChannelFactory;
import io.grpc.ManagedChannel;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Remote scan operator backed by the region server gRPC API.
 */
public class RemoteScanOperator extends Operator {

    private final String tableName;
    private final String regionId;
    private final String serverHost;
    private final int serverPort;
    private final byte[] startKey;
    private final byte[] endKey;
    private final Table tableSchema;
    private final String[] outputColumns;

    private ManagedChannel channel;
    private RegionServerServiceGrpc.RegionServerServiceBlockingStub stub;
    private Iterator<RegionServerProto.ScanResponse> responseIterator;
    private Iterator<KeyValue> kvIterator;
    private Iterator<Row> rowIterator;
    private Row nextRow;
    private boolean opened;

    public RemoteScanOperator(String tableName, String regionId,
                              String serverHost, int serverPort,
                              byte[] startKey, byte[] endKey,
                              Table tableSchema, String[] outputColumns) {
        this.tableName = tableName;
        this.regionId = regionId;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.startKey = startKey;
        this.endKey = endKey;
        this.tableSchema = tableSchema;
        this.outputColumns = outputColumns;
    }

    public RemoteScanOperator(String tableName, String regionId,
                              String serverHost, int serverPort,
                              byte[] startKey, byte[] endKey) {
        this(tableName, regionId, serverHost, serverPort, startKey, endKey, null, null);
    }

    @Override
    public void open() {
        channel = GrpcChannelFactory.forAddress(serverHost, serverPort);
        stub = RegionServerServiceGrpc.newBlockingStub(channel);

        RegionServerProto.ScanRequest.Builder requestBuilder = RegionServerProto.ScanRequest.newBuilder()
            .setRegionId(regionId);
        if (tableName != null) {
            requestBuilder.setTableName(tableName);
        }
        if (startKey != null) {
            requestBuilder.setStartKey(com.google.protobuf.ByteString.copyFrom(startKey));
        }
        if (endKey != null) {
            requestBuilder.setEndKey(com.google.protobuf.ByteString.copyFrom(endKey));
        }

        responseIterator = stub.scan(requestBuilder.build());
        kvIterator = new GrpcKeyValueIterator();
        rowIterator = new KeyValueRowIterator(kvIterator, tableSchema, getOutputColumns());
        opened = true;
        prefetch();
    }

    @Override
    public Row nextRow() {
        if (!opened) {
            open();
        }
        if (nextRow == null) {
            return null;
        }
        Row result = nextRow;
        prefetch();
        return result;
    }

    @Override
    public boolean hasMore() {
        if (!opened) {
            open();
        }
        return nextRow != null;
    }

    @Override
    public void close() {
        opened = false;
        nextRow = null;
        kvIterator = null;
        rowIterator = null;
        responseIterator = null;
        channel = null;
        stub = null;
    }

    @Override
    public void reset() {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        if (outputColumns != null && outputColumns.length > 0) {
            return outputColumns;
        }
        if (tableSchema != null && tableSchema.getColumns() != null) {
            List<com.minisql.common.model.Column> columns = tableSchema.getColumns();
            String[] names = new String[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                names[i] = columns.get(i).getName();
            }
            return names;
        }
        return new String[] {"rowKey", "family", "qualifier", "value"};
    }

    private void prefetch() {
        nextRow = rowIterator != null && rowIterator.hasNext() ? rowIterator.next() : null;
    }

    private KeyValue convertProtoToKeyValue(CommonProto.KeyValue kvProto) {
        KeyValue kv = new KeyValue();
        kv.setRowKey(kvProto.getRowKey().toByteArray());
        kv.setFamily(kvProto.getColumnFamily());
        kv.setQualifier(kvProto.getQualifier());
        kv.setTimestamp(kvProto.getTimestamp());
        kv.setValue(kvProto.getValue().toByteArray());
        kv.setType(kvProto.getType() == CommonProto.KeyValueType.PUT ? KeyValue.Type.PUT : KeyValue.Type.DELETE);
        return kv;
    }

    private class GrpcKeyValueIterator implements Iterator<KeyValue> {
        private Iterator<CommonProto.KeyValue> currentBatch;
        private KeyValue nextKv;

        private GrpcKeyValueIterator() {
            prefetch();
        }

        @Override
        public boolean hasNext() {
            return nextKv != null;
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KeyValue result = nextKv;
            prefetch();
            return result;
        }

        private void prefetch() {
            while (true) {
                if (currentBatch != null && currentBatch.hasNext()) {
                    nextKv = convertProtoToKeyValue(currentBatch.next());
                    return;
                }
                if (responseIterator != null && responseIterator.hasNext()) {
                    RegionServerProto.ScanResponse response = responseIterator.next();
                    if (!response.getStatus().getSuccess()) {
                        throw new RuntimeException("Scan response error: " + response.getStatus().getMessage());
                    }
                    currentBatch = response.getKeyValuesList().iterator();
                    continue;
                }
                nextKv = null;
                return;
            }
        }
    }
}
