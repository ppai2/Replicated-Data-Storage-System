package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by prasan on 4/21/15.
 */
public class Message implements Serializable {

    private String sender;
    private String receiver;
    private MessageType msgType;
    private String key;
    private String value;
    String query_key;
    String originalSender;
    String sendToOriginalSender;
    String myPortSender;
    String node;
    ArrayList<String> message_array = new ArrayList<String>();

    Message(String sender, String receiver) {
        this.sender = sender;
        this.receiver = receiver;
        msgType = MessageType.START;
    }

    public void setKeyValue(String key,String value){
        this.key = key;
        this.value = value;
    }

    public String getKey(){
        return key;
    }

    public String getValue(){
        return value;
    }

    public void setMsgType(MessageType msgType) {
        this.msgType = msgType;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public String getReceiver() {
        return receiver;
    }

    public void setQueryKey(String query_key) {
        this.query_key = query_key;
    }

    public String getQuery_key() {
        return query_key;
    }

    public void setOriginalSender(String port){
        originalSender = port;
    }

    public String getOriginalSender(){
        return originalSender;
    }

    public void setToSend(String sendToOriginalSender) {
        this.sendToOriginalSender = sendToOriginalSender;
    }

    public void setToSender(String myPortSender) {
        this.myPortSender = myPortSender;
    }

    public String getToSender() {
        return myPortSender;
    }

    public void setNodeValue(String node) {
        this.node = node;
    }

    public String getNodeValue() {
        return node;
    }

}

enum MessageType {
    START, INSERT_REQUEST, INSERT_REPLICA, QUERY_INIT_REQUEST, QUERY_REQUEST, QUERY_ALL, QUERY_FOUND, DELETE_REQUEST, DELETE_ALL, GET_FAIL_MESSAGES_PARENT, GET_FAIL_MESSAGES_SUCCESSOR, FAILED_MESSAGES
}
