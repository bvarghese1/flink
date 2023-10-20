package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;

import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for implementing restore tests for {@link ExecNode}.You can generate json compiled
 * plan and a savepoint for the latest node version by running {@link
 * RestoreTestBase#generateTestSetupFiles(TableTestProgram)} which is disabled by default.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class RestoreTestBase implements TableTestProgramRunner {

    private final Class<? extends ExecNode> execNodeUnderTest;

    protected RestoreTestBase(Class<? extends ExecNode> execNodeUnderTest) {
        this.execNodeUnderTest = execNodeUnderTest;
    }

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestKind.SOURCE_WITH_RESTORE_DATA,
                TestKind.SINK_WITH_RESTORE_DATA
        );
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(
                TestKind.SQL
        );
    }

    private @TempDir Path tmpDir;

    private List<Integer> getVersions() {
        return ExecNodeMetadataUtil.extractMetadataFromAnnotation(execNodeUnderTest).stream()
                .map(ExecNodeMetadata::version)
                .collect(Collectors.toList());
    }

    private int getCurrentVersion() {
        return ExecNodeMetadataUtil.latestAnnotation(execNodeUnderTest).version();
    }

    private Stream<Arguments> createSpecs() {
        return getVersions().stream()
                .flatMap(
                        version -> supportedPrograms().stream().map(p -> Arguments.of(version, p)));
    }

    /**
     * Execute this test to generate test files. Remember to be using the correct branch when
     * generating the test files.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("supportedPrograms")
    public void generateTestSetupFiles(TableTestProgram program) throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id =
                    TestValuesTableFactory.registerData(sourceTestStep.dataBeforeRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("finite", "false");
            options.put("disable-lookup", "true");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final CompletableFuture<Object> future = new CompletableFuture<>();
            futures.add(future);
            final String tableName = sinkTestStep.name;
            TestValuesTableFactory.registerLocalRawResultsObserver(
                    tableName,
                    (integer, strings) -> {
                        final boolean canTakeSavepoint = sinkTestStep.expectedBeforeRestore.test(
                                TestValuesTableFactory.getRawResults(tableName));
                        if (canTakeSavepoint) {
                            future.complete(null);
                        }
                    });
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        final SqlTestStep sqlTestStep = program.getRunSqlTestStep();

        final CompiledPlan compiledPlan = tEnv.compilePlanSql(sqlTestStep.sql);
        compiledPlan.writeToFile(getPlanPath(program, getCurrentVersion()));

        final TableResult tableResult = compiledPlan.execute();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        final JobClient jobClient = tableResult.getJobClient().get();
        final String savepoint =
                jobClient
                        .stopWithSavepoint(false, tmpDir.toString(), SavepointFormatType.DEFAULT)
                        .get();
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        final Path savepointPath = Paths.get(new URI(savepoint));
        final Path savepointDirPath = getSavepointPath(program, getCurrentVersion());
        Files.createDirectories(savepointDirPath);
        Files.move(savepointPath, savepointDirPath, StandardCopyOption.ATOMIC_MOVE);
    }

    @ParameterizedTest
    @MethodSource("createSpecs")
    void testRestore(int version, TableTestProgram program) throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        final SavepointRestoreSettings restoreSettings =
                SavepointRestoreSettings.forPath(
                        getSavepointPath(program, version).toString(), false, RestoreMode.NO_CLAIM);
        SavepointRestoreSettings.toConfiguration(restoreSettings, settings.getConfiguration());
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanRestore.IDENTIFIER);
        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id =
                    TestValuesTableFactory.registerData(sourceTestStep.dataAfterRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("disable-lookup", "true");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        final CompiledPlan compiledPlan =
                tEnv.loadPlan(PlanReference.fromFile(getPlanPath(program, version)));
        compiledPlan.execute().await();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            assertThat(TestValuesTableFactory.getRawResults(sinkTestStep.name))
                    .matches(l -> sinkTestStep.expectedAfterRestore.test((List<Row>) l));
        }
    }

    private Path getPlanPath(TableTestProgram program, int version) {
        return Paths.get(
                getTestResourceDirectory(program, version) + "/plan/" + program.id + ".json");
    }

    private Path getSavepointPath(TableTestProgram program, int version) {
        return Paths.get(getTestResourceDirectory(program, version) + "/savepoint/");
    }

    private String getTestResourceDirectory(TableTestProgram program, int version) {
        return System.getProperty("user.dir") + "/src/test/resources/" + program.id + "-" + version;
    }
}
