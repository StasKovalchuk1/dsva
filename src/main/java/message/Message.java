package message;

import java.io.Serializable;

public class Message implements Serializable {
    public enum MessageType {
        REQUEST,
        REPLY,
        UPDATE,
        NODE_ID
    }

    private MessageType type;
    private int timestamp;
    private String senderId;
    private String updatedValue;

    public Message(MessageType type, int timestamp, String senderId) {
        this.type = type;
        this.timestamp = timestamp;
        this.senderId = senderId;
    }

    public Message(MessageType type, int timestamp, String senderId, String updatedValue) {
        this.type = type;
        this.timestamp = timestamp;
        this.senderId = senderId;
        this.updatedValue = updatedValue;
    }

    public MessageType getType() {
        return type;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getUpdatedValue() {
        return updatedValue;
    }
}
