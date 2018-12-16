package pku;

import java.io.File;

public class Topic {

    private final String fileName;
    public final String bucket;

    private final PersistenceFile logFile; // Log File
    private final WriteBuffer writeBuffer; // Write Buffer

    public Topic(String bucket) {
        this.bucket = bucket;
        fileName = bucket;
        // topic dir
        logFile = new PersistenceFile(fileName);
        writeBuffer = new WriteBuffer(logFile);
    }

    // for Producer
    public WriteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    // for Consumer
    public PersistenceFile getLogFile() {
        return logFile;
    }

}