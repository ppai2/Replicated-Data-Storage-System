package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = "TAG";
    boolean wait = true;
    static final String ERROR = "ERROR";
    static final int SERVER_PORT = 10000;
    public SQLiteDatabase database;
    public String myPort;
    Socket socket = null;
    static final String REMOTE_PORT0 = "5554";
    static final String REMOTE_PORT1 = "5556";
    static final String REMOTE_PORT2 = "5558";
    static final String REMOTE_PORT3 = "5560";
    static final String REMOTE_PORT4 = "5562";
    String predecessor = null;
    String successor = null;
    String replica1 = null;
    String replica2 = null;
    String predec1 = null;
    String predec2 = null;
    String success1 = null;
    Cursor finalCursor;
    private static final String KEY_FIELD = "key";
    List<String> portChord = new ArrayList<String>();
    HashMap<String, Object> keyHashMap = new HashMap<>();
    HashMap<String, Boolean> flagHashMap = new HashMap<>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int row = 0;
        if ((selection.equals("@")) || (selection.equals("\"@\""))) {
            row =  database.delete("MESSAGE_ORDER", null, null);

        } else {
            String keyNodeCheck = nodeCheck(selection);
            Message deleteSendNode = new Message(keyNodeCheck, keyNodeCheck);
            deleteSendNode.setMsgType(MessageType.DELETE_REQUEST);
            deleteSendNode.setToSender(keyNodeCheck);
            deleteSendNode.setQueryKey(selection);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteSendNode);

            String replica1 = portChord.get((portChord.indexOf(keyNodeCheck) + 1) % portChord.size());
            Message deleteSend1 = new Message(replica1, replica1);
            deleteSend1.setMsgType(MessageType.DELETE_REQUEST);
            deleteSend1.setToSender(replica1);
            deleteSend1.setQueryKey(selection);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteSend1);

            String replica2 = portChord.get((portChord.indexOf(keyNodeCheck) + 2) % portChord.size());
            Message deleteSend2 = new Message(replica2, replica2);
            deleteSend2.setMsgType(MessageType.DELETE_REQUEST);
            deleteSend2.setToSender(replica2);
            deleteSend2.setQueryKey(selection);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteSend2);
        }
        return row;
    }


    private void deleteMe(String selection) {
        int row;
        row = database.delete("MESSAGE_ORDER", "key=\"" + selection + "\"", null);
        Log.d("TAG DELETE", row + " row deleted from replica nodes");
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public String nodeCheck(String key_hash) {
        String node = null;
        try {
            if (genHash("5562").compareTo(genHash(key_hash)) >= 0) {
                node = "5562";
            } else if (genHash("5556").compareTo(genHash(key_hash)) >= 0) {
                node = "5556";
            } else if (genHash("5554").compareTo(genHash(key_hash)) >= 0) {
                node = "5554";
            } else if (genHash("5558").compareTo(genHash(key_hash)) >= 0) {
                node = "5558";
            } else if (genHash("5560").compareTo(genHash(key_hash)) >= 0) {
                node = "5560";
            } else {
                node = "5562";
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return node;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String key_hash = key;
        Log.d("TAG INSERT", "MyPort node value : " + myPort);

        try {
            Log.d("TAG INSERT", "Insert request for key:" + key_hash);
            Log.d("TAG INSERT", "Hash of Key :" + genHash(key_hash));
            String node1 = nodeCheck(key_hash);
            Log.d("TAG INSERT", "Node value for insert: " + node1);

            if (node1.equals("5562")) {
                replica1 = "5556";
                replica2 = "5554";
                Log.d("TAG INSERT", "Calling NodesToBeSent function");
                NodesToBeSent(node1, replica1, replica2, key_hash, value);

            } else if (node1.equals("5556")) {
                replica1 = "5554";
                replica2 = "5558";
                Log.d("TAG INSERT", "Calling NodesToBeSent function");
                NodesToBeSent(node1, replica1, replica2, key_hash, value);

            } else if (node1.equals("5554")) {
                replica1 = "5558";
                replica2 = "5560";
                Log.d("TAG INSERT", "Calling NodesToBeSent function");
                NodesToBeSent(node1, replica1, replica2, key_hash, value);

            } else if (node1.equals("5558")) {
                replica1 = "5560";
                replica2 = "5562";
                Log.d("TAG INSERT", "Calling NodesToBeSent function");
                NodesToBeSent(node1, replica1, replica2, key_hash, value);

            } else if (node1.equals("5560")) {
                replica1 = "5562";
                replica2 = "5556";
                Log.d("TAG INSERT", "Calling NodesToBeSent function");
                NodesToBeSent(node1, replica1, replica2, key_hash, value);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    private void NodesToBeSent(String node1, String replica1, String replica2, String key, String value) {
        Message main_node = new Message(node1, node1);
        main_node.setToSender(node1);
        main_node.setMsgType(MessageType.INSERT_REQUEST);
        main_node.setKeyValue(key, value);
        Log.d("TAG INSERT", "Message object sent to port " + node1);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, main_node);

        Message replicate1 = new Message(replica1, replica1);
        replicate1.setToSender(replica1);
        replicate1.setMsgType(MessageType.INSERT_REPLICA);
        replicate1.setKeyValue(key, value);
        Log.d("TAG INSERT", "Message object sent to replica port " + replica1);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicate1);

        Message replicate2 = new Message(replica2, replica2);
        replicate2.setToSender(replica2);
        replicate2.setMsgType(MessageType.INSERT_REPLICA);
        replicate2.setKeyValue(key, value);
        Log.d("TAG INSERT", "Message object sent to replica port " + replica2);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicate2);

    }

    private void insertMe(String key, String value) {
        ContentValues cv = new ContentValues();
        String node = nodeCheck(key);
        cv.put("key",key);
        cv.put("value", value);
        cv.put("node", node);
        Log.d("TAG INSERT", "Inserting values in the database  " + key + " " + value + " " + node);
        database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", cv, SQLiteDatabase.CONFLICT_REPLACE);

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        DBHelperClass dbHelperClass = new DBHelperClass(getContext());
        database = dbHelperClass.getWritableDatabase();
        Log.d(TAG, "Database created");

        try {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf(Integer.parseInt(portStr));
            Log.d(TAG, "My Port ID is :" + myPort);
            ServerSocket serverSocket = null;
            Message message = new Message(myPort, myPort);
            portChord.add(0, "5562");
            portChord.add(1, "5556");
            portChord.add(2, "5554");
            portChord.add(3, "5558");
            portChord.add(4, "5560");
            Log.d(TAG, "portChord contains the ports in the sequence " + portChord);
            Log.d(TAG, "Message object created for myPort " + myPort);

            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if (myPort.equals("5562")) {
                predec1 = "5558";
                predec2 = "5560";
                success1 = "5556";
            } else if (myPort.equals("5556")) {
                predec1 = "5560";
                predec2 = "5562";
                success1 = "5554";
            } else if (myPort.equals("5554")) {
                predec1 = "5562";
                predec2 = "5556";
                success1 = "5558";
            } else if (myPort.equals("5558")) {
                predec1 = "5556";
                predec2 = "5554";
                success1 = "5560";
            } else if (myPort.equals("5560")) {
                predec1 = "5554";
                predec2 = "5558";
                success1 = "5562";
            }

            Message parent1 = new Message(predec1, predec1);
            parent1.setToSender(predec1);
            parent1.setOriginalSender(myPort);
            parent1.setMsgType(MessageType.GET_FAIL_MESSAGES_PARENT);
            Log.d("TAG INSERT", "Message object sent to parent 1: " + predec1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, parent1);

            Message parent2 = new Message(predec2, predec2);
            parent2.setToSender(predec2);
            parent2.setOriginalSender(myPort);
            parent2.setMsgType(MessageType.GET_FAIL_MESSAGES_PARENT);
            Log.d("TAG INSERT", "Message object sent to parent 2: " + predec2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, parent2);

            Message successor1 = new Message(success1, success1);
            successor1.setToSender(success1);
            successor1.setOriginalSender(myPort);
            successor1.setMsgType(MessageType.GET_FAIL_MESSAGES_SUCCESSOR);
            Log.d("TAG INSERT", "Message object sent to successor 1: " + success1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successor1);


            message.setToSender(myPort);
            message.setMsgType(MessageType.START);
            Log.d(TAG, "Message sent for myPort " + myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables("MESSAGE_ORDER");
        Log.d("TAG QUERY", "Before the query @ if condition statement");
        Log.d("TAG QUERY", "selection query :" + selection);

        if ((selection.equals("@")) || (selection.equals("\"@\""))) {
                /*
                Local dump of the database for @ query
                 */
            Log.d("TAG QUERY", "Inside the query @ condition statement");
            Cursor c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
            c2.moveToFirst();
            Log.d("TAG QUERY", "Cursor returned : " + c2.getCount());

            MatrixCursor c4 = convertCurToMat(c2);
            Log.d("TAG QUERY", "Matrix Cursor returned : " + c4.getCount());
            return c4;

        } else if ((selection.equals("*")) || (selection.equals("\"*\""))) {
                /*
                Global dump of the database for * query
                 */
            Log.d("TAG QUERY ALL", "Inside the query * condition statement");
            Cursor c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
            c2.moveToFirst();
            ArrayList<String> query_array = new ArrayList<String>();
            while (!(c2.isAfterLast())) {
                query_array.add(c2.getString(0));
                query_array.add(c2.getString(1));
                query_array.add(c2.getString(2));
                c2.moveToNext();
            }

            Message mess_query_array = new Message(successor, successor);
            mess_query_array.message_array = query_array;
            mess_query_array.setToSender(successor);
            mess_query_array.setMsgType(MessageType.QUERY_ALL);
            mess_query_array.setOriginalSender(myPort);
            mess_query_array.setQueryKey("*");
            Log.d("TAG QUERY ALL", "Message object to be sent to " + successor);
            Log.d("TAG QUERY ALL", "first array list size " + mess_query_array.message_array.size());

            ClientTask tempTask = new ClientTask();
            tempTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query_array);
            try {
                String st = tempTask.get();
                Log.d("Final cursor count", "Final cursor count: " + finalCursor.getCount());

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            MatrixCursor c5 = convertCurToMat(finalCursor);
            Log.d("Final cursor count", "Final cursor count: " + c5.getCount());
            return c5;

        } else {
            /* Query for selection parameter */
            Cursor c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
            c1.moveToFirst();

            if (c1.getCount() == 0) {
                Log.d("TAG QUERY", "Empty query in IF loop for key: " + selection);
                String node = nodeCheck(selection);
                Log.d("TAG QUERY", "Key in the database of node :" + node);
                if (!(node.equals(myPort))) {
                    Message mess_query = new Message(node, node);
                    mess_query.setToSender(node);
                    mess_query.setQueryKey(selection);
                    mess_query.setMsgType(MessageType.QUERY_INIT_REQUEST);
                    mess_query.setOriginalSender(myPort);
                    Log.d("TAG QUERY", "Query request forwarded to node: " + node + " for key : " + selection + " from:" + mess_query.getOriginalSender());

                    ClientTask tempTask = new ClientTask();
                    tempTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query);
                        /*try {
                            String st = tempTask.get();
                            c1 = finalCursor;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }*/

                    flagHashMap.put(selection, true);
                    Log.d("TAG QUERY FOUND", "Changing the flag wait to true");
                    while (flagHashMap.get(selection)) {

                    }

                    c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
                    c1.moveToFirst();

                    MatrixCursor c2 = convertCurToMat(c1);

                    database.delete("MESSAGE_ORDER", "key=?",new String[]{mess_query.getQuery_key()});
                    Log.d("TAG QUERY FOUND", "Key :" + mess_query.getQuery_key() + " inserted in the database and deleted from :" + mess_query.getOriginalSender());
                    //Log.d("TAG QUERY FOUND", "Key :" + c2.getString(0) + "  value :" + c2.getString(1));
                    Log.d("TAG QUERY FOUND", "Cursor count: " + c1.getCount() + "  MatrixCursor count :" + c2.getCount());
                    return c2;
                }
                else {
                    Log.e(TAG,"No Results found in me????");
                    c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
                    c1.moveToFirst();
                    MatrixCursor c3 = convertCurToMat(c1);
                }
            } else {
                c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
                c1.moveToFirst();
                MatrixCursor c3 = convertCurToMat(c1);
                //delete(null, mess_query.getKey(), null);
                //Log.d("TAG QUERY FOUND", "Key :" + c3.getString(0) + "  value :" + c3.getString(1));
                Log.d("TAG QUERY FOUND", "Cursor count: " + c1.getCount() + "  MatrixCursor count :" + c3.getCount());
                return c3;

            }
            return c1;
        }
    }

    private MatrixCursor convertCurToMat(Cursor c1) {
        MatrixCursor matrixCursor = new MatrixCursor((new String[]{"key", "value"}));
        c1.moveToFirst();
        while(!(c1.isAfterLast())) {
            matrixCursor.addRow(new String[]{c1.getString(0),c1.getString(1)});
            c1.moveToNext();
        }
        Log.d("Cursor count","Cursor count: " + c1.getCount());
        Log.d("Cursor count","Matrix cursor count: " + matrixCursor.getCount());
        return matrixCursor;
    }

    private Cursor query(String selection) {
        /* Block to forward query to node in case the data is not present in the current node */
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables("MESSAGE_ORDER");
        Log.d("TAG QUERY SELECTION", "Inside the query function for checking the selection of key :" + selection);
        Cursor c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
        c1.moveToFirst();
        return c1;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask< ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ObjectInputStream objectInputStream1 = null;

            while (!serverSocket.isClosed()) {
                try {
                    socket = serverSocket.accept();
                    objectInputStream1 = new ObjectInputStream(socket.getInputStream());

                    Log.d(TAG, "Inside ServerTask method");
                    Message m1 = (Message) objectInputStream1.readObject();

                    publishProgress(m1);

                } catch (IOException e) {
                    Log.e(TAG, "Socket IOException at server end");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        protected void onProgressUpdate(Message... messages) {
            Message m1 = messages[0];


            Log.d(TAG, "Message received by " + m1.getReceiver());
            Log.d(TAG, "Inside onProgressUpdate block of serverTask");
            if (m1.getMsgType().equals(MessageType.INSERT_REQUEST) || m1.getMsgType().equals(MessageType.INSERT_REPLICA)) {
                Log.d("TAG INSERT", "before insertion in the database");
                Log.d("TAG INSERT", "value to be inserted in the database " + m1.getKey() + " " + m1.getValue());
                insertMe(m1.getKey(), m1.getValue());
                Log.d("TAG INSERT", "after insertion in the database");

            } else if (m1.getMsgType().equals(MessageType.QUERY_ALL)) {
                SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
                sqLiteQueryBuilder.setTables("MESSAGE_ORDER");

                if (!(m1.getOriginalSender().equals(myPort))) {
                    Log.d("TAG QUERY ALL", "Inside the query end else condition statement for * condition for myPort :" + myPort);
                    Cursor c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
                    c2.moveToFirst();
                    ArrayList<String> query_array = m1.message_array;
                    while(!(c2.isAfterLast())) {
                        query_array.add(c2.getString(0));
                        query_array.add(c2.getString(1));
                        query_array.add(c2.getString(2));
                        c2.moveToNext();
                    }
                    Log.d("TAG QUERY ALL", "Inside the query end else condition statement for * condition for myPort :" + myPort);
                    Message mess_query_array = new Message(successor, successor);
                    mess_query_array.setToSender(successor);
                    mess_query_array.setMsgType(MessageType.QUERY_ALL);
                    mess_query_array.setOriginalSender(m1.getOriginalSender());
                    mess_query_array.setQueryKey("*");
                    mess_query_array.message_array = query_array;
                    Log.d("TAG QUERY ALL", "Message object to be sent to successor :" + successor + " of myPort: " + myPort);
                    Log.d("TAG QUERY ALL", "first array list size " + mess_query_array.message_array.size());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query_array);

                } else {
                    int f=0;
                    Log.d("FINAL ARRAY", "Final array list size" + m1.message_array.size());
                    MatrixCursor matrixCursor = new MatrixCursor((new String[]{"key", "value"}));
                    while(f<m1.message_array.size()) {
                        matrixCursor.addRow(new String[]{m1.message_array.get(f),m1.message_array.get(f+1)});
                        f=f+3;
                    }
                    finalCursor = matrixCursor;
                    Log.d("Cursor count","Cursor count: " + matrixCursor.getCount());
                    wait=false;

                }
            } else if (m1.getMsgType().equals(MessageType.QUERY_INIT_REQUEST)) {
                /*
                    Query request to be forwarded to successor in case data is not found
                    */
                Log.d("TAG QUERY SELECTION", "Inside the query QUERY_INIT_REQUEST and QUERY_REQUEST");
                Log.d("TAG QUERY SELECTION", "from:" + myPort + " key:" + m1.getQuery_key() + " origin:" + m1.getOriginalSender());
                Log.d("TAG QUERY SELECTION", "query for key: " + m1.getQuery_key());
                Cursor c = query(m1.getQuery_key());
                Log.d("TAG QUERY FOUND", "cursor size returned :" + c.getCount());
                Log.d("TAG QUERY FOUND", "cursor contains :" + c.getString(0) + " " + c.getString(1));

                if(c!=null){
                    Log.d("TAG QUERY FOUND","Sending to original sender:"+m1.getOriginalSender()+" key:"+m1.getQuery_key());
                    m1.setKeyValue(c.getString(c.getColumnIndex(KEY_FIELD)),c.getString(c.getColumnIndex("value")));
                    m1.setMsgType(MessageType.QUERY_FOUND);
                    m1.setToSender(m1.getOriginalSender());
                    Log.d("TAG QUERY FOUND", "Message object sent to port :" + m1.getOriginalSender());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1);
                }

            } else if(m1.getMsgType().equals(MessageType.QUERY_FOUND)) {
                Log.d("TAG QUERY FOUND", "QUERY FOUND AND RECEIVED key:" + m1.getKey());
                insertMe(m1.getKey(), m1.getValue());
                flagHashMap.put(m1.getKey(), false);

            } else if(m1.getMsgType().equals(MessageType.GET_FAIL_MESSAGES_PARENT)) {
                Cursor parent_cursor = queryFailedMessages(m1.getReceiver());
                parent_cursor.moveToFirst();
                Log.d("QUERY FAIL MSGS parent", "QUERY FUNC TO BE CALLED FOR NODE :" + m1.getReceiver());
                Log.d("QUERY FAIL MSGS parent", "Cursor count :" + parent_cursor.getCount());
                while(!(parent_cursor.isAfterLast())) {
                    String key_parent = parent_cursor.getString(0);
                    String value_parent = parent_cursor.getString(1);
                    String node_parent = parent_cursor.getString(2);

                    Log.d("Cursor count", "Cursor count :" + parent_cursor.getCount());
                    Log.d("QUERY FAIL MSGS replica", "Cursor contains:" + parent_cursor.getString(0) + " " + parent_cursor.getString(1) + " " + parent_cursor.getString(2));
                    m1.message_array.add(key_parent);
                    m1.message_array.add(value_parent);
                    m1.message_array.add(node_parent);
                    Log.d("QUERY FAIL MSGS parent", "Messages added in the array: " + m1.message_array.size());
                    parent_cursor.moveToNext();
                }
                Log.d("QUERY FAIL MSGS parent", "Message array size :" + m1.message_array.size());
                Message mess_query_array1 = new Message(m1.getOriginalSender(), m1.getOriginalSender());
                mess_query_array1.setMsgType(MessageType.FAILED_MESSAGES);
                mess_query_array1.setToSender(m1.getOriginalSender());
                mess_query_array1.message_array = m1.message_array;
                Log.d("QUERY FAIL MSGS replica", "Messages added in the parent mess query array: " + mess_query_array1.message_array.size());
                Log.d("QUERY FAIL MSGS parent", "Message array size :" + m1.getOriginalSender());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query_array1);

            } else if(m1.getMsgType().equals(MessageType.GET_FAIL_MESSAGES_SUCCESSOR)) {
                Cursor replica_cursor = queryFailedMessages(m1.getOriginalSender());
                replica_cursor.moveToFirst();
                Log.d("QUERY FAIL MSGS replica", "QUERY FUNC TO BE CALLED FOR NODE :" + m1.getReceiver());
                Log.d("QUERY FAIL MSGS replica", "Cursor count :" + replica_cursor.getCount());
                while(!(replica_cursor.isAfterLast())) {
                    String key_parent = replica_cursor.getString(0);
                    String value_parent = replica_cursor.getString(1);
                    String node_parent = replica_cursor.getString(2);

                    Log.d("QUERY FAIL MSGS replica", "Cursor count2 :" + replica_cursor.getCount());
                    Log.d("TAGGGGGGGGGGGG replica", "Cursor contains:" + replica_cursor.getString(0) + " " + replica_cursor.getString(1) + " " + replica_cursor.getString(2));
                    m1.message_array.add(key_parent);
                    m1.message_array.add(value_parent);
                    m1.message_array.add(node_parent);
                    Log.d("QUERY FAIL MSGS replica", "Messages added in the array: " + m1.message_array.size());
                    replica_cursor.moveToNext();
                }
                Log.d("QUERY FAIL MSGS replica", "Message array size :" + m1.message_array.size());
                Message mess_query_array2 = new Message(m1.getOriginalSender(), m1.getOriginalSender());
                mess_query_array2.setMsgType(MessageType.FAILED_MESSAGES);
                mess_query_array2.message_array = m1.message_array;
                Log.d("QUERY FAIL MSGS replica", "Messages added in the replica     mess query array: " + mess_query_array2.message_array.size());
                mess_query_array2.setToSender(m1.getOriginalSender());
                Log.d("QUERY FAIL MSGS replica", "Message sent to :" + m1.getOriginalSender());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query_array2);

            } else if(m1.getMsgType().equals(MessageType.FAILED_MESSAGES)) {
                int f = 0;
                Log.d("FINAL ARRAY", "Final array list size" + m1.message_array.size());
                if (m1.message_array.size() != 0) {
                    while (f < m1.message_array.size()) {
                        Log.d("TAG", "Message contains:" + m1.message_array.get(f) + " " + m1.message_array.get(f + 1) + " " + m1.message_array.get(f + 2));
                        insertFailMessages(m1.message_array.get(f), m1.message_array.get(f + 1), m1.message_array.get(f + 2));
                        f = f + 3;
                    }
                }
                Log.d("FAILED MESSAGES INSERT", "Inside failed messages insert block");
                Log.d("FAILED MESSAGES INSERT", "Insert function called for inserting the keys in the database ");

            } else if(m1.getMsgType().equals(MessageType.DELETE_REQUEST)) {
                Log.d("TAG QUERY FOUND", "QUERY FOUND AND RECEIVED key:" + m1.getQuery_key());
                deleteMe(m1.getQuery_key());

            } else {
                try {
                    if (portChord.contains(myPort)) {
                        replica1 = portChord.get((portChord.indexOf(myPort) + 1) % portChord.size());
                        successor = replica1;
                        replica2 = portChord.get((portChord.indexOf(myPort) + 2) % portChord.size());
                        Log.d(TAG, "Replica nodes for myPort " + myPort + " are " + replica1 + " and " + replica2);
                        Log.d(TAG, "genhash of myPort " + genHash(myPort));
                        if (myPort.equals("5562")) {
                            predecessor = portChord.get(portChord.size() - 1);
                            Log.d(TAG, "Predecessor node for myPort " + myPort + " is " + predecessor);
                        } else {
                            predecessor = portChord.get((portChord.indexOf(myPort) - 1) % portChord.size());
                            Log.d(TAG, "Predecessor node for myPort " + myPort + " is " + predecessor);
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void insertFailMessages(String key1, String value1, String node1) {
        ContentValues cv = new ContentValues();
        cv.put("key",key1);
        cv.put("value", value1);
        cv.put("node", node1);
        Log.d("TAG FAIL INSERT", "Inserting values in the database  " + key1 + " " + value1 + " " + node1);
        database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", cv, SQLiteDatabase.CONFLICT_REPLACE);

    }

    private Cursor queryFailedMessages(String receiver) {
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables("MESSAGE_ORDER");
        Log.d("TAG FAIL QUERY", "Inside the query function for checking the failed messages for node: " + receiver);
        Cursor c1 = sqLiteQueryBuilder.query(database, null, "node=\"" + receiver + "\"", null, null, null, null);
        c1.moveToFirst();
        Log.d("TAG", "Cursor count in query func: " + c1.getCount() );
        return c1;

    }

    private class ClientTask extends AsyncTask<Message, Void, String> {

        protected String doInBackground(Message... msgs) {
            Message msgToSend = msgs[0];
            Socket socket;
            String remotePort;
            ObjectOutputStream objectOutputStream;
            if (msgToSend.getMsgType().equals(MessageType.START) || msgToSend.getMsgType().equals(MessageType.INSERT_REQUEST) ||
                    msgToSend.getMsgType().equals(MessageType.INSERT_REPLICA) || msgToSend.getMsgType().equals(MessageType.QUERY_INIT_REQUEST) ||
                    msgToSend.getMsgType().equals(MessageType.QUERY_FOUND) || msgToSend.getMsgType().equals(MessageType.QUERY_ALL) ||
                    msgToSend.getMsgType().equals(MessageType.GET_FAIL_MESSAGES_PARENT) || msgToSend.getMsgType().equals(MessageType.GET_FAIL_MESSAGES_SUCCESSOR) ||
                    msgToSend.getMsgType().equals(MessageType.FAILED_MESSAGES) || msgToSend.getMsgType().equals(MessageType.DELETE_REQUEST)) {
                remotePort = msgToSend.getToSender();
                if (remotePort.equals("5554") || remotePort.equals("5556") || remotePort.equals("5558") || remotePort.equals("5560") || remotePort.equals("5562")) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(remotePort) * 2));
                        Log.d("TAG NODE TO BE SENT", "inside client task for port " + remotePort);
                        objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();
                        objectOutputStream.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.d(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        if (msgToSend.getMsgType().equals(MessageType.QUERY_ALL) || msgToSend.getMsgType().equals(MessageType.QUERY_INIT_REQUEST)) {
                            try {
                                Log.e("TAG NODE FAIL", "inside ClientTask socket IOException");
                                String successor_port = portChord.get((portChord.indexOf(remotePort) + 1) % portChord.size());
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(successor_port) * 2));
                                Log.d("TAG NODE FAIL", "Node failed for remotePort :" + remotePort);
                                Log.d("TAG NODE FAIL", "Sending from client task for successor port " + successor_port);
                                objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                objectOutputStream.writeObject(msgToSend);
                                objectOutputStream.flush();
                                objectOutputStream.close();
                                socket.close();
                            } catch (UnknownHostException e11) {
                                Log.d(TAG, "ClientTask UnknownHostException");
                            } catch (IOException e11) {
                                Log.d(TAG, "ClientTask socket IOException");
                                e.printStackTrace();
                            }
                        }

                    }
                }
            }/* else if ((msgToSend.getMsgType().equals(MessageType.GET_FAIL_MESSAGES_PARENT) || msgToSend.getMsgType().equals(MessageType.FAILED_MESSAGES))) {
                String parent = msgToSend.getToSender();
                if (parent.equals("5554") || parent.equals("5556") || parent.equals("5558") || parent.equals("5560") || parent.equals("5562")) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(parent) * 2));
                        Log.d("TAG NODE TO BE SENT", "inside client task for port " + parent);
                        objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();
                        objectOutputStream.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.d(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.d("TAG PARENT", "ClientTask Socket IOException");
                    }
                }
            } else if ((msgToSend.getMsgType().equals(MessageType.GET_FAIL_MESSAGES_SUCCESSOR) || msgToSend.getMsgType().equals(MessageType.FAILED_MESSAGES))) {
                String replicaNode = msgToSend.getToSender();
                if (replicaNode.equals("5554") || replicaNode.equals("5556") || replicaNode.equals("5558") || replicaNode.equals("5560") || replicaNode.equals("5562")) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replicaNode) * 2));
                        Log.d("TAG NODE TO BE SENT", "inside client task for port " + replicaNode);
                        objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();
                        objectOutputStream.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.d(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.d("TAG REPLICA", "ClientTask Socket IOException");
                    }
                }
            }*/



            if((msgToSend.getMsgType().equals(MessageType.QUERY_ALL)) && msgToSend.getOriginalSender().equals(myPort)){
                /*
                Wait block for original sender for * selection
                 */
                Log.d("TAG WAIT","START");
                wait = true;
                while(wait){
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Log.d("TAG WAIT","OVER");
            }
            return null;
        }

    }
}
