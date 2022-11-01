package com.example;

import static org.apache.flink.table.api.Expressions.row;

import com.google.common.base.Joiner;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UDFTest.class);

    final String TEST_FUNCTIONS_LOCATION = "src/test/resources/functions";


    @Test
    public void shouldGenerateFlinkJobForInteractiveQueryWithUDFSuccessfully() throws Exception {
        final Path jarPath = Paths.get(TEST_FUNCTIONS_LOCATION, "user-functions.jar");
        final String jarPathString = String.format("%s%s", "file://", jarPath.toAbsolutePath());
        final List<String> jarPathStrings = List.of(jarPathString);
        final ClassLoader loader = getFunctionsClassLoader(jarPathStrings, this.getClass().getClassLoader());

        final Configuration config = new Configuration();
        config.set(PipelineOptions.CLASSPATHS, jarPathStrings);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String functionClassName = "util.LowerCase";

        Thread.currentThread().setContextClassLoader(loader);
        final Class<? extends UserDefinedFunction> clazz =
            (Class<? extends UserDefinedFunction>) Class.forName(functionClassName, true, loader);
        tEnv.createTemporarySystemFunction("LowerCase", clazz);

        final Table table = tEnv.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD("name", DataTypes.STRING())
            ),
            row(1, "ABC"),
            row(2L, "ABCDE")
        );

        final CloseableIterator<Row> iter = tEnv.sqlQuery("SELECT LowerCase(name) as name FROM " + table).execute().collect();
        final List<Row> list = new ArrayList<>();
        iter.forEachRemaining(list::add);

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(list);
    }

    public static ClassLoader getFunctionsClassLoader(
        final List<String> functionSourcePaths, final ClassLoader parent) {
        final URL[] urls = functionSourcePaths.stream()
            .map(f -> {
                try {
                    return new URL(f);
                } catch (MalformedURLException e) {
                    LOGGER.error(
                        "Failed creating URL for function source [functionSourcePath={}, error={}]",
                        f,
                        e.getMessage());
                    throw new RuntimeException("Failed to load user-defined function", e);
                }
            })
            .toArray(URL[]::new);
        LOGGER.info(
            "Creating class loader for user-defined functions [classes={}]",
            Joiner.on(", ").join(urls));
        return URLClassLoader.newInstance(urls, parent);
    }
}
