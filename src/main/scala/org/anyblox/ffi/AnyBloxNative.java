package org.anyblox.ffi;

public class AnyBloxNative {
    private static final String NATIVE_LIB_NAME = "anyblox_jni";

    private static final String libraryToLoad = System.mapLibraryName(NATIVE_LIB_NAME);
    private static boolean loaded = false;
    private static volatile Throwable loadErr = null;

    static {
        try {
            load();
        } catch (Throwable th) {
            System.err.println("Failed to load anyblox_jni library: " + th.getMessage());
            loadErr = th;
        }
    }

    public static native void init();

    public static native long createRuntime(long config);

    public static native long createConfigBuilder();

    public static native void dropConfigBuilder(long builder);

    public static native void configBuilderSetWasmCacheLimit(long builder, long limit);

    public static native void configBuilderSetThreadVirtualMemoryLimit(long builder, long limit);

    public static native void configBuilderSetLogLevel(long builder, int logLevel);

    public static native void configBuilderSetLogDirectory(long builder, String path);

    public static native void configBuilderCompileWithDebug(long builder, boolean value);

    public static native long configBuilderFinish(long builder);

    public static native void dropConfig(long config);

    public static native void dropRuntime(long runtime);

    public static native long[] runtimeDecodeInit(long runtime, long bundle, boolean validateUtf8);

    public static native long[] runtimeDecodeInitWithProjection(long runtime, long bundle, long projection, boolean validateUtf8);

    public static native long jobRunAndBlock(long runtime, long job, long firstTuple, long tupleCount);

    public static native void dropJob(long job);

    public static native long bundleOpenSelfContained(int fd, long len);

    public static native long bundleOpenExtensionAndData(int anybloxFd, long anybloxLen, int dataFd, long dataLen);

    public static native long[] bundleDecoder(long bundle);

    public static native Object[] bundleMetadataData(long bundle);

    public static native Object[] bundleMetadataDecoder(long bundle);

    public static native long bundleMetadataSchema(long bundle);

    public static native void dropBundle(long bundle);

    static synchronized void load() throws Throwable {
        if (loaded) {
            return;
        }
        if (loadErr != null) {
            throw loadErr;
        }

        System.loadLibrary(NATIVE_LIB_NAME);
        loaded = true;

        init();
    }
}
