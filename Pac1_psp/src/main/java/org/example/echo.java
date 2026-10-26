package org.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class echo {

    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        String rol = args.length == 0
                ? "supervisor"
                : args[0].toLowerCase(Locale.ROOT);

        switch (rol) {
            case "supervisor" -> runSupervisor();
            case "echo" -> runEcho();
            case "worker" -> runWorker(args);
            case "producer" -> runProducer();
            case "consumer" -> runConsumer();
            case "test" -> runTests();
            default -> {
                System.err.println("Rol no reconegut: " + rol);
                System.exit(2);
            }
        }
    }

    private static List<String> javaCommand(String rol, String... args) {
        List<String> command = new ArrayList<>();
        command.add(Path.of(
                System.getProperty("java.home"),
                "bin",
                "java"
        ).toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(echo.class.getName());
        command.add(rol);
        Collections.addAll(command, args);
        return command;
    }

    private static void runSupervisor() throws Exception {
        log("SUPERVISOR", "Iniciant supervisor de processos");

        runEchoExample();
        runWorkers();
        runPipe();

        log("SUPERVISOR", "Supervisor finalitzat");
    }

    private static void runEchoExample() throws Exception {
        log("SUPERVISOR", "Executant exemple echo");

        ProcessResult result = execute(
                javaCommand("echo"),
                List.of("Ebre", "processos", "FI"),
                Duration.ofSeconds(5)
        );

        printResult("ECHO", result);
    }

    private static void runWorkers() throws Exception {
        log("SUPERVISOR", "Executant workers amb un màxim de dos processos simultanis");

        ExecutorService executor = Executors.newFixedThreadPool(2);

        List<Callable<ProcessResult>> tasks = List.of(
                () -> execute(
                        javaCommand("worker", "ALPHA", "1200", "0"),
                        List.of(),
                        Duration.ofSeconds(5)
                ),
                () -> execute(
                        javaCommand("worker", "BETA", "900", "7"),
                        List.of(),
                        Duration.ofSeconds(5)
                ),
                () -> execute(
                        javaCommand("worker", "GAMMA", "700", "0"),
                        List.of(),
                        Duration.ofSeconds(5)
                )
        );

        try {
            List<Future<ProcessResult>> futures = new ArrayList<>();

            for (Callable<ProcessResult> task : tasks) {
                futures.add(executor.submit(task));
            }

            for (Future<ProcessResult> future : futures) {
                try {
                    printResult("WORKER", future.get());
                } catch (ExecutionException e) {
                    System.err.println("Error executant worker: " + e.getCause());
                }
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private static void runPipe() throws Exception {
        log("SUPERVISOR", "Iniciant comunicació producer-consumer");

        Process producer = new ProcessBuilder(javaCommand("producer"))
                .start();

        Process consumer = new ProcessBuilder(javaCommand("consumer"))
                .start();

        long producerPid = producer.pid();
        long consumerPid = consumer.pid();

        Instant startedAt = Instant.now();

        List<String> producerErrors =
                Collections.synchronizedList(new ArrayList<>());

        List<String> consumerOutput =
                Collections.synchronizedList(new ArrayList<>());

        List<String> consumerErrors =
                Collections.synchronizedList(new ArrayList<>());

        ExecutorService readers =
                Executors.newVirtualThreadPerTaskExecutor();

        Future<?> producerErrorReader = readers.submit(() ->
                readLines(
                        producer.getErrorStream(),
                        producerErrors,
                        "PRODUCER STDERR"
                )
        );

        Future<?> consumerOutputReader = readers.submit(() ->
                readLines(
                        consumer.getInputStream(),
                        consumerOutput,
                        "CONSUMER STDOUT"
                )
        );

        Future<?> consumerErrorReader = readers.submit(() ->
                readLines(
                        consumer.getErrorStream(),
                        consumerErrors,
                        "CONSUMER STDERR"
                )
        );

        List<String> transferredLines = new ArrayList<>();

        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                producer.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                );
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(
                                consumer.getOutputStream(),
                                StandardCharsets.UTF_8
                        )
                )
        ) {
            String line;

            while ((line = reader.readLine()) != null) {
                transferredLines.add(line);
                writer.write(line);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Error en la comunicació pipe: " + e.getMessage());
        }

        boolean producerFinished = producer.waitFor(8, TimeUnit.SECONDS);

        if (!producerFinished) {
            terminate(producer);
        }

        boolean consumerFinished = consumer.waitFor(8, TimeUnit.SECONDS);

        if (!consumerFinished) {
            terminate(consumer);
        }

        producerErrorReader.get(3, TimeUnit.SECONDS);
        consumerOutputReader.get(3, TimeUnit.SECONDS);
        consumerErrorReader.get(3, TimeUnit.SECONDS);

        Duration duration = Duration.between(startedAt, Instant.now());

        System.out.println("PIPE producerPid=" + producerPid);
        System.out.println("PIPE consumerPid=" + consumerPid);
        System.out.println("PIPE producerExit=" +
                (producerFinished ? producer.exitValue() : "TIMEOUT"));
        System.out.println("PIPE consumerExit=" +
                (consumerFinished ? consumer.exitValue() : "TIMEOUT"));
        System.out.println("PIPE durationMs=" + duration.toMillis());
        System.out.println("PIPE transferredLines=" + transferredLines);

        for (String line : producerErrors) {
            System.out.println(line);
        }

        for (String line : consumerOutput) {
            System.out.println(line);
        }

        for (String line : consumerErrors) {
            System.out.println(line);
        }

        readers.shutdownNow();
        readers.awaitTermination(2, TimeUnit.SECONDS);
    }

    private static ProcessResult execute(
            List<String> command,
            List<String> inputLines,
            Duration timeout
    ) throws Exception {

        Instant startedAt = Instant.now();

        Process process = new ProcessBuilder(command).start();

        long pid = process.pid();
        long ppid = process.toHandle()
                .parent()
                .map(handle -> handle.pid())
                .orElse(-1L);

        List<String> stdout =
                Collections.synchronizedList(new ArrayList<>());

        List<String> stderr =
                Collections.synchronizedList(new ArrayList<>());

        ExecutorService readers =
                Executors.newVirtualThreadPerTaskExecutor();

        Future<?> stdoutReader = readers.submit(() ->
                readLines(process.getInputStream(), stdout, "STDOUT")
        );

        Future<?> stderrReader = readers.submit(() ->
                readLines(process.getErrorStream(), stderr, "STDERR")
        );

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        process.getOutputStream(),
                        StandardCharsets.UTF_8
                )
        )) {
            for (String line : inputLines) {
                writer.write(line);
                writer.newLine();
            }
        }

        boolean finished = process.waitFor(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS
        );

        if (!finished) {
            terminate(process);
        }

        stdoutReader.get(3, TimeUnit.SECONDS);
        stderrReader.get(3, TimeUnit.SECONDS);

        int exitCode = finished ? process.exitValue() : -1;

        Duration duration = Duration.between(startedAt, Instant.now());

        readers.shutdownNow();
        readers.awaitTermination(2, TimeUnit.SECONDS);

        return new ProcessResult(
                String.join(" ", command),
                pid,
                ppid,
                exitCode,
                duration,
                !finished,
                new ArrayList<>(stdout),
                new ArrayList<>(stderr),
                startedAt
        );
    }

    private static void terminate(Process process) throws InterruptedException {
        process.destroy();

        if (!process.waitFor(500, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            process.waitFor(2, TimeUnit.SECONDS);
        }
    }

    private static void readLines(
            java.io.InputStream stream,
            List<String> destination,
            String source
    ) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8)
        )) {
            String line;

            while ((line = reader.readLine()) != null) {
                destination.add(
                        "[" + LocalDateTime.now().format(FORMAT) + "] "
                                + source + ": " + line
                );
            }
        } catch (IOException e) {
            destination.add(
                    "[" + LocalDateTime.now().format(FORMAT) + "] "
                            + source + ": Error de lectura: " + e.getMessage()
            );
        }
    }

    private static void printResult(String label, ProcessResult result) {
        System.out.println("[" + label + "] PID=" + result.pid());
        System.out.println("[" + label + "] PPID=" + result.ppid());
        System.out.println("[" + label + "] EXIT=" + result.exitCode());
        System.out.println("[" + label + "] TIMEOUT=" + result.timeout());
        System.out.println("[" + label + "] DURATION_MS=" +
                result.duration().toMillis());
        System.out.println("[" + label + "] COMMAND=" + result.command());

        for (String line : result.stdout()) {
            System.out.println(line);
        }

        for (String line : result.stderr()) {
            System.out.println(line);
        }
    }

    private static void runEcho() throws IOException {
        logErr("ECHO", "Procés echo iniciat");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        System.in,
                        StandardCharsets.UTF_8
                )
        )) {
            String line;

            while ((line = reader.readLine()) != null) {
                System.out.println(line.toUpperCase(Locale.ROOT));

                if (line.equalsIgnoreCase("FI")) {
                    break;
                }
            }
        }

        logErr("ECHO", "Procés echo finalitzat");
    }

    private static void runWorker(String[] args) throws InterruptedException {
        String name = args.length > 1 ? args[1] : "WORKER";
        long delay = args.length > 2
                ? Long.parseLong(args[2])
                : 500;
        int exitCode = args.length > 3
                ? Integer.parseInt(args[3])
                : 0;

        logErr(name, "Iniciat");

        long step = Math.max(1, delay / 3);

        for (int i = 1; i <= 3; i++) {
            Thread.sleep(step);
            logErr(name, "Pas " + i + " de 3");
        }

        logErr(name, "Finalitzat amb codi " + exitCode);

        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static void runProducer() throws IOException {
        logErr("PRODUCER", "Iniciant emissió de dades");

        List<String> names = List.of(
                "Anna",
                "Biel",
                "Carla",
                "David",
                "Ebre"
        );

        for (String name : names) {
            System.out.println(name);
        }

        logErr("PRODUCER", "Emissió finalitzada");
    }

    private static void runConsumer() throws IOException {
        int lines = 0;
        int characters = 0;

        logErr("CONSUMER", "Esperant dades");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        System.in,
                        StandardCharsets.UTF_8
                )
        )) {
            String line;

            while ((line = reader.readLine()) != null) {
                lines++;
                characters += line.length();
            }
        }

        System.out.printf(
                "CONSUMER_RESULT lines=%d characters=%d%n",
                lines,
                characters
        );

        logErr("CONSUMER", "Consum de dades finalitzat");
    }

    private static void runTests() throws Exception {
        log("TEST", "Iniciant proves");

        ProcessResult echoResult = execute(
                javaCommand("echo"),
                List.of("prova", "FI"),
                Duration.ofSeconds(3)
        );

        if (echoResult.timeout()
                || echoResult.exitCode() != 0
                || echoResult.stdout().stream()
                .noneMatch(line -> line.endsWith("PROVA"))) {
            throw new AssertionError("Ha fallat la prova d'echo");
        }

        ProcessResult workerResult = execute(
                javaCommand("worker", "TEST_ERROR", "100", "7"),
                List.of(),
                Duration.ofSeconds(3)
        );

        if (workerResult.timeout() || workerResult.exitCode() != 7) {
            throw new AssertionError("Ha fallat la prova del codi d'eixida");
        }

        ProcessResult timeoutResult = execute(
                javaCommand("worker", "TEST_TIMEOUT", "10000", "0"),
                List.of(),
                Duration.ofMillis(300)
        );

        if (!timeoutResult.timeout()) {
            throw new AssertionError("Ha fallat la prova de timeout");
        }

        System.out.println("TEST echo: OK");
        System.out.println("TEST exit code: OK");
        System.out.println("TEST timeout: OK");
        System.out.println("Totes les proves han finalitzat correctament");
    }

    private static void logErr(String role, String message) {
        System.err.println(
                "[" + LocalDateTime.now().format(FORMAT) + "] "
                        + "role=" + role
                        + " pid=" + ProcessHandle.current().pid()
                        + " ppid=" + ProcessHandle.current()
                        .parent()
                        .map(ProcessHandle::pid)
                        .orElse(-1L)
                        + " thread=" + Thread.currentThread().getName()
                        + " message=" + message
        );
    }

    private static void log(String role, String message) {
        System.out.println(
                "[" + LocalDateTime.now().format(FORMAT) + "] "
                        + "role=" + role
                        + " pid=" + ProcessHandle.current().pid()
                        + " ppid=" + ProcessHandle.current()
                        .parent()
                        .map(ProcessHandle::pid)
                        .orElse(-1L)
                        + " thread=" + Thread.currentThread().getName()
                        + " message=" + message
        );
    }

    private record ProcessResult(
            String command,
            long pid,
            long ppid,
            int exitCode,
            Duration duration,
            boolean timeout,
            List<String> stdout,
            List<String> stderr,
            Instant startedAt
    ) {
        private ProcessResult {
            stdout = List.copyOf(stdout);
            stderr = List.copyOf(stderr);
        }
    }
}