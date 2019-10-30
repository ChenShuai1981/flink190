//package com.caselchen.flink;
//
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.SqlParserException;
//import org.apache.flink.table.api.TableEnvironment;
//
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.List;
//
//public class SqlSubmit {
//
//    private TableEnvironment tEnv;
//    private String sqlFilePath;
//    private String workingSpace;
//
//    public static void main(String[] args) throws Exception {
//        CliOptions options = CliOptionsParser.parseClient(args);
//        SqlSubmit sqlSubmit = new SqlSubmit(options);
//        sqlSubmit.run();
//    }
//
//    private SqlSubmit(CliOptions options) {
//        this.sqlFilePath = options.getSqlFilePath();
//        this.workingSpace = options.getWorkingSpace();
//    }
//
//    private void run() throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//
//        this.tEnv = TableEnvironment.create(settings);
//        List<String> sql = Files.readAllLines(Paths.get(this.workingSpace + "/" + this.sqlFilePath));
//        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
//        for (SqlCommandCall call : calls) {
//            callCommand(call);
//        }
//        tEnv.execute("SQL Job");
//    }
//
//    private void callCommand(SqlCommandCall cmdCall) {
//        switch (cmdCall.command) {
//            case SET:
//                callSet(cmdCall);
//                break;
//            case CREATE_TABLE:
//                callCreateTable(cmdCall);
//                break;
//            case INSERT_INTO:
//                callInsertInto(cmdCall);
//                break;
//            default:
//                throw new RuntimeException("Unsupported command: " + cmdCall.command);
//
//        }
//    }
//
//    private void callSet(SqlCommandCall cmdCall) {
//        String key = cmdCall.operands[0];
//        String value = cmdCall.operands[1];
//        tEnv.getConfig().getConfiguration().setString(key, value);
//    }
//
//    private void callCreateTable(SqlCommandCall cmdCall) {
//        String ddl = cmdCall.operands[0];
//        try {
//            tEnv.sqlUpdate(ddl);
//        } catch (SqlParserException e) {
//            throw new RuntimeException("SQL parsed failed: \n" + ddl + "\n", e)
//        }
//    }
//
//    private void callInsertInto(SqlCommandCall cmdCall) {
//        String dml = cmdCall.operands[0];
//        try {
//            tEnv.sqlUpdate(dml);
//        } catch (SqlParserException e) {
//            throw new RuntimeException("SQL parsed failed: \n" + dml + "\n", e)
//        }
//    }
//}
