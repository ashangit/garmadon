package com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs;

import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class AbstractFileSystemTracer {
    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;
    private static final ConcurrentHashMap METHOD_CACHE = new ConcurrentHashMap<Object, Method>();
    private static final ConcurrentHashMap FIELD_CACHE = new ConcurrentHashMap<ClassLoader, Field>();
    private static BiConsumer<Long, Object> eventHandler;

    private static TypeDescription pathTD =
        new TypeDescription.Latent("org.apache.hadoop.fs.Path", Opcodes.ACC_PUBLIC, TypeDescription.Generic.OBJECT);

    protected AbstractFileSystemTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Instrumentation instrumentation, BiConsumer<Long, Object> eventConsumer) {

        initEventHandler(eventConsumer);

        new AbstractFileSystemTracer.ReadTracer().installOn(instrumentation);
        new AbstractFileSystemTracer.WriteTracer().installOn(instrumentation);
        new AbstractFileSystemTracer.RenameTracer().installOn(instrumentation);
        new AbstractFileSystemTracer.DeleteTracer().installOn(instrumentation);
        new AbstractFileSystemTracer.ListStatusTracer().installOn(instrumentation);
    }

    public static ConcurrentHashMap getFieldCache() {
        return FIELD_CACHE;
    }

    public static ConcurrentHashMap getMethodCache() {
        return METHOD_CACHE;
    }

    public static Method getMethod(ClassLoader classLoader, String clazz, String method, Class<?>... parameterTypes) {
        return (Method) getMethodCache().computeIfAbsent(classLoader + clazz + method,
            k -> {
                try {
                    Class classzz = classLoader.loadClass(clazz);
                    return classzz.getMethod(method, parameterTypes);
                } catch (NoSuchMethodException | ClassNotFoundException ignored) {
                    return null;
                }
            });
    }

    public static Field getField(ClassLoader classLoader, String clazz, String field) {
        return (Field) getFieldCache().computeIfAbsent(classLoader + clazz + field, k -> {
            try {
                Class classzz = classLoader.loadClass(clazz);
                Field fieldComputed = classzz.getDeclaredField(field);
                fieldComputed.setAccessible(true);
                return fieldComputed;
            } catch (ClassNotFoundException | NoSuchFieldException ignored) {
                return null;
            }
        });
    }

    public static void initEventHandler(BiConsumer<Long, Object> eventConsumer) {
        AbstractFileSystemTracer.eventHandler = eventConsumer;
    }

    public static class DeleteTracer extends MethodTracer {


        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.fs.Hdfs");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("delete").and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(AbstractFileSystemTracer.DeleteTracer.class);
        }

        @RuntimeType
        public static Object intercept(
            @SuperCall Callable<?> zuper,
            @This Object o,
            @Argument(0) Object dst) throws Exception {
            return callDistributedFileSystem(zuper, o, null, dst.toString(), FsAction.DELETE.name());
        }
    }

    public static class ReadTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.fs.Hdfs");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("open").and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(AbstractFileSystemTracer.ReadTracer.class);
        }

        @RuntimeType
        public static Object intercept(
            @SuperCall Callable<?> zuper,
            @This Object o,
            @Argument(0) Object dst) throws Exception {
            return callDistributedFileSystem(zuper, o, null, dst.toString(), FsAction.READ.name());
        }
    }

    public static class RenameTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.fs.Hdfs");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("renameInternal");
        }

        @Override
        protected Implementation newImplementation() {
            return to(AbstractFileSystemTracer.RenameTracer.class);
        }

        @RuntimeType
        public static Object intercept(
            @SuperCall Callable<?> zuper,
            @This Object o,
            @Argument(0) Object src,
            @Argument(1) Object dst) throws Exception {
            return callDistributedFileSystem(zuper, o, src.toString(), dst.toString(), FsAction.RENAME.name());
        }
    }

    public static class WriteTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.fs.Hdfs");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("createInternal").and(takesArguments(7)).and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(AbstractFileSystemTracer.WriteTracer.class);
        }

        @RuntimeType
        public static Object intercept(
            @SuperCall Callable<?> zuper,
            @This Object o,
            @Argument(0) Object dst) throws Exception {
            return callDistributedFileSystem(zuper, o, null, dst.toString(), FsAction.WRITE.name());
        }
    }

    public static class ListStatusTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.fs.Hdfs");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("listStatus").and(
                takesArguments(pathTD)
            );
        }

        @Override
        protected Implementation newImplementation() {
            return to(AbstractFileSystemTracer.ListStatusTracer.class);
        }

        @RuntimeType
        public static Object intercept(
            @SuperCall Callable<?> zuper,
            @This Object o,
            @Argument(0) Object dst) throws Exception {
            return callDistributedFileSystem(zuper, o, null, dst.toString(), FsAction.LIST_STATUS.name());
        }
    }

    private static Object callDistributedFileSystem(@SuperCall Callable<?> zuper, @This Object o, String src, String dst, String fsAction) throws Exception {
        ClassLoader classLoader = o.getClass().getClassLoader();
        Field dfsField = getField(classLoader, "org.apache.hadoop.fs.Hdfs", "dfs");
        Object dfs = dfsField.get(o);
        Field ugiField = getField(classLoader, "org.apache.hadoop.hdfs.DFSClient", "ugi");
        Object ugi = ugiField.get(dfs);
        Method getShortUserName = getMethod(classLoader, "org.apache.hadoop.security.UserGroupInformation", "getShortUserName");
        Method getUri = getMethod(classLoader, "org.apache.hadoop.fs.Hdfs", "getUri");
        return executeMethod(zuper, getUri.invoke(o).toString(), src, dst, fsAction, (String) getShortUserName.invoke(ugi));
    }

    private static Object executeMethod(@SuperCall Callable<?> zuper, String uri, String src, String dst, String fsAction, String username) throws Exception {
        long startTime = System.nanoTime();
        DataAccessEventProtos.FsEvent.Status status = DataAccessEventProtos.FsEvent.Status.SUCCESS;
        try {
            Object result = zuper.call();
            if (Boolean.FALSE.equals(result)) {
                status = DataAccessEventProtos.FsEvent.Status.FAILURE;
            }
            return result;
        } catch (Exception e) {
            status = DataAccessEventProtos.FsEvent.Status.FAILURE;
            throw e;
        } finally {
            long elapsedTime = (System.nanoTime() - startTime) / NANOSECONDS_PER_MILLISECOND;
            sendFsEvent(uri, src, dst, fsAction, username, elapsedTime, status);
        }
    }

    private static void sendFsEvent(String uri, String src, String dst, String fsAction, String username, long durationMillis,
                                    DataAccessEventProtos.FsEvent.Status status) {
        DataAccessEventProtos.FsEvent.Builder eventBuilder = DataAccessEventProtos.FsEvent
            .newBuilder();

        eventBuilder.setAction(fsAction)
            .setDstPath(dst)
            .setUri(uri)
            .setMethodDurationMillis(durationMillis)
            .setStatus(status);

        if (username != null) {
            eventBuilder.setHdfsUser(username);
        }

        if (src != null) {
            eventBuilder.setSrcPath(src);
        }

        eventHandler.accept(System.currentTimeMillis(), eventBuilder.build());

    }
}
