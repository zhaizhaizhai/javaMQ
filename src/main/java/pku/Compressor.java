package pku;


import java.util.zip.Deflater;

// 单例实现压缩工具
public class Compressor extends Deflater {
    private static class SingletonHolder {
        private static final Compressor INSTANCE = new Compressor();
    }

    private Compressor() {
    }

    public static final Compressor getInstance() {
        return SingletonHolder.INSTANCE;
    }
}
