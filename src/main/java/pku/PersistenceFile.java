package pku;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


public class PersistenceFile {
    public final String fileName;

    private  RandomAccessFile randomAccessFile;
    private  FileChannel fileChannel;

    public PersistenceFile( String fileName) {
        this.fileName = fileName;
        File file = new File(Constants.ROOT_PATH,fileName);
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

}