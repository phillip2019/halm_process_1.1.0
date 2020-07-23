//package com.aikosolar.app;
//
//import com.alibaba.fastjson.JSON;
//import com.google.common.base.Joiner;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HConstants;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.MD5Hash;
//
//import java.text.ParseException;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@SuppressWarnings("all")
//@Slf4j
//public class HbaseSink extends RichSinkFunction<String> {
//    private int bufferSpace;
//    private int numReplicas;
//    private transient Connection hbaseConn;
//    private BufferedMutator hbaseTable;
//    FinkHbaseConfig parameterTool;
//    Map<String, FinkHbaseConfig.SchemaV> schemaMap;
//    Map<String, FinkHbaseConfig.SchemaV> upSchemaMap;
//    Map<String, AtomicInteger> alarmMap = Maps.newConcurrentMap();
//
//
//    public HbaseSink(int bufferSpace) {
//        if (bufferSpace < 2097152) {//默认2m，如果小于则置为2m
//            bufferSpace = 2097152;
//        }
//        this.bufferSpace = bufferSpace;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        parameterTool = (FinkHbaseConfig) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//        conf.set(HConstants.ZOOKEEPER_QUORUM, parameterTool.getHbaseZookeeperHost());
//        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, parameterTool.getHbaseZookeeperPort());
//        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parameterTool.getHbaseZookeeperPath());
//        ExecutorService threads = Executors.newFixedThreadPool(4);
//        this.hbaseConn = ConnectionFactory.createConnection(conf, threads);
//
//        schemaMap = parameterTool.getSchemaMap();
//        upSchemaMap = parameterTool.getUpSchemaMap();
//
//        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
//            @Override
//            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
//                for (int i = 0; i < e.getNumExceptions(); i++) {
//                    log.error("Failed to sent put " + e.getRow(i) + ".");
//                    Exception exception = new AlarmException("put", AlarmLevel.WARN, "put 失败 " + e.getMessage());
//                    sendAlarm(exception, new String(e.getRow(i).getRow()));
//                }
//            }
//        };
//        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(parameterTool.getHbaseTable()));
//        params.writeBufferSize(this.bufferSpace);
//
//        hbaseTable = this.hbaseConn.getBufferedMutator(params);
//        alarmMap.put("all", new AtomicInteger());
//        alarmMap.put("warn", new AtomicInteger());
//        alarmMap.put("critical", new AtomicInteger());
//        ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);
//        schedule.scheduleAtFixedRate(()->{
//            flush();
//        },30L,30L, TimeUnit.SECONDS);
//    }
//
//    @Override
//    public void close() throws Exception {
//        try {
//            flush();
//        } finally {
//            try {
//                if (this.hbaseTable != null) {
//                    this.hbaseTable.close();
//                }
//                if (this.hbaseConn != null) {
//                    hbaseConn.close();
//                }
//            } catch (Exception e) {
//                log.error("Error while closing session.", e);
//            }
//        }
//    }
//
//
//    @Override
//    public void invoke(String line) throws Exception {
//        try {
//            Map<String, String> dataMap = new HashMap<>();
//            Map<String, Object> upDataMap = getUpDataMap(line, dataMap);
//            String rowkey = getRowkey(upDataMap);//
//            put(rowkey, dataMap);
//        } catch (Exception e) {
//            log.error(e.getMessage(), e);
//            sendAlarm(e, line);
//        }
//    }
//
//    private String getRowkey(Map<String, Object> dataMap) {
//        String rowketT = parameterTool.getRowkeyHash();
//        String[] ks = parameterTool.getRowkey().split(",");
//        StringBuilder sb = new StringBuilder();
//        for (String s : ks) {
//            if (s.startsWith("/")) {
//                sb.append(dataMap.get(s.replace("/", "")));
//            } else {
//                sb.append(s);
//            }
//        }
//        String rowkey;
//        if ("1".equals(parameterTool.getRowkeyHash())) {
//            rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(sb.toString()));
//        } else {
//            rowkey = sb.toString();
//        }
//        return rowkey;
//    }
//
//    /**
//     * 批量插入数据
//     *
//     * @return
//     * @throws ParseException
//     * @tableName 表名字
//     * @info 需要写入的信息
//     */
//    public void put(String rowkey, Map<String, String> dataMap) throws Exception {
//        try {
//            Put put = new Put(Bytes.toBytes(rowkey));
//            for (Map.Entry<String, FinkHbaseConfig.SchemaV> e : this.schemaMap.entrySet()) {
//                String fv = dataMap.get(e.getKey());
//                String[] fcArr = e.getKey().split(":");
//                if (fcArr.length != 2) {//格式不满足
//                    throw new AlarmException("put", AlarmLevel.CRITICAL, "schema格式无效,schema: " + Joiner.on(",").join(this.schemaMap.keySet()));
//                }
//                String family = fcArr[0];
//                String cell = fcArr[1];
//                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(cell), Bytes.toBytes(fv));
//
//            }
////            Mutation mutation = new
////            mutations.add(put);
//            hbaseTable.mutate(put);
//        } catch (Exception e) {
//            if (e instanceof AlarmException) {
//                throw e;
//            }
//            throw new AlarmException("put", AlarmLevel.WARN, "put 失败 ");
//        }
//
//    }
//
//
//    private void sendAlarm(Exception e, String data) {
//        if (e instanceof AlarmException) {
//            AlarmException alarmException = (AlarmException) e;
//            alarmMap.get("all").incrementAndGet();
//            sendCountAlarm();
//            if (alarmException.getLevel() == AlarmLevel.WARN) {
//                sendWarnAlarm(e, data);
//            } else if (alarmException.getLevel() == AlarmLevel.CRITICAL) {
//                sendCriticalAlarm(e, data);
//            }
//        }
//    }
//
//    private void sendWarnAlarm(Exception e, String data) {
//        alarmMap.get("warn").incrementAndGet();
//        if (alarmMap.get("warn").intValue() < 10) {
//            if (e instanceof AlarmException) {
//                AlarmException alarmException = (AlarmException) e;
//                send(data, String.format("[%s] %s", AlarmLevel.WARN.name(), alarmException.getMsg()));
//            }
//        }
//    }
//
//    private void sendCriticalAlarm(Exception e, String data) {
//        alarmMap.get("critical").incrementAndGet();
//        if (alarmMap.get("critical").intValue() < 3) {
//            if (e instanceof AlarmException) {
//                AlarmException alarmException = (AlarmException) e;
//                send(data, String.format("[%s] %s", AlarmLevel.CRITICAL.name(), alarmException.getMsg()));
//            }
//        }
//    }
//
//    private void sendCountAlarm() {
//        //错误总数超过一个阈值 则发送报警
//        int count = alarmMap.get("all").intValue();
//        if (count > 10000) {
//            //大于10000 则停止处理
//            send("", "异常数已经大于10000，处理进程退出");
//            System.exit(1);//异常退出
//        } else if (count == 5000) {
//            send("", "异常数已经大于5000");
//        } else if (count == 2000) {
//            send("", "异常数已经大于2000");
//        } else if (count == 1000) {
//            send("", "异常数已经大于1000");
//        } else if (count == 500) {
//            send("", "异常数已经大于500");
//        } else if (count == 100) {
//            send("", "异常数已经大于100");
//        } else if (count == 10) {
//            send("", "异常数已经大于10");
//        }
//
//
//    }
//
//    public void send(String data, String msg) {
//
//        //发送错误消息
//        Alarm alarm = new Alarm();
//        alarm.setCode(Alarm.AlarmType.schema_changed_write_fail.getCode());
//        alarm.setData(data);
//        alarm.setPipelineId(parameterTool.getPipelineId());
//        alarm.setInstanceName(parameterTool.getInstanceName());
//        alarm.setStageName(parameterTool.getStageName());
//        alarm.setRunMode(parameterTool.getRunMode());
//        alarm.setEventTime(System.currentTimeMillis());
//        alarm.setMsg(msg);
//
//        KafkaProducer.send(parameterTool.getAlarmKafkaConfig().get("bootstrap.servers").toString(),
//                parameterTool.getAlarmKafkaConfig().get("topic").toString(),
//                alarm);
//    }
//
//
//    public void flush() {
//        try {
//            log.info("---------------flush------------");
//            if (hbaseTable != null) {
//                this.hbaseTable.flush();
//            }
//        }catch (Exception e){
//            log.info("---------------flush fail------------");
//            log.error(e.getMessage(),e);
//        }
//
//    }
//
//
//    private Map<String, Object> getUpDataMap(String line, Map<String, String> dataMap) throws Exception {
//        String dataFormat = parameterTool.getDataFormat();
//        Map<String, Object> upDataMap = null;
//        if ("JSON".equals(dataFormat)) {
//            upDataMap = JSON.parseObject(line, Map.class);
//            if (upDataMap.size() != this.upSchemaMap.size()) {
//                throw new AlarmException("getDataMap", AlarmLevel.CRITICAL, String.format("数据和schema不匹配:schema:[ %s ] ", parameterTool.getUpstreamSchema()));
//            }
//            for (Map.Entry<String, FinkHbaseConfig.SchemaV> e : this.upSchemaMap.entrySet()) {
//                if (upDataMap.get(e.getKey()) == null) {
//                    throw new AlarmException("getDataMap", AlarmLevel.CRITICAL, String.format("数据和schema不匹配:schema:[ %s ] ", parameterTool.getUpstreamSchema()));
//                }
//            }
//        } else if ("DELIMITED".equals(dataFormat)) {
//            String[] dataStrArr = line.split(parameterTool.getDelimitor());
//            upDataMap = new HashMap<>();
//            if (dataStrArr.length != this.upSchemaMap.size()) {
//                throw new AlarmException("getDataMap", AlarmLevel.CRITICAL, String.format("数据和schema不匹配:schema:[ %s ] ", parameterTool.getUpstreamSchema()));
//            }
//            int i = 0;
//            for (Map.Entry<String, FinkHbaseConfig.SchemaV> e : this.upSchemaMap.entrySet()) {
//                upDataMap.put(e.getKey(), dataStrArr[i]);
//                i++;
//            }
//        }
//        //获取数据
//        for (Map.Entry<String, FinkHbaseConfig.SchemaV> entry : this.schemaMap.entrySet()) {
//            String origField = entry.getValue().getOrigFiled();
//            if (StringUtils.isNotBlank(origField) && origField.startsWith("/")) {
//                String reKey = origField.replace("/", "");
//                dataMap.put(entry.getKey(), String.valueOf(upDataMap.get(reKey)));
//            } else {
//                dataMap.put(entry.getKey(), entry.getValue().getDefaultV());
//            }
//        }
//        return upDataMap;
//    }
//
//
//    class SchemaChangedException extends Exception {
//
//    }
//
//    @Getter
//    @Setter
//    class PrimaryKeyChangedException extends Exception {
//        boolean needUpdateSchema;
//
//        public PrimaryKeyChangedException() {
//            this(false);
//        }
//
//        public PrimaryKeyChangedException(boolean needUpdateSchema) {
//            this.needUpdateSchema = needUpdateSchema;
//        }
//
//    }
//
//}
