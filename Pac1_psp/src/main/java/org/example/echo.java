package org.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Exemple IPC amb Java 21: un supervisor envia text a un procés fill.
 * Compilar: javac --release 21 echo.java
 * Executar supervisor: java echo
 * Executar només el fill: java echo echo
 */
public class echo {
    // Cada JVM executa main: sense arguments és el pare; amb "echo" és el fill.
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            runSupervisor();
        } else if (args[0].equals("echo")) {
            runEcho();
        } else {
            throw new IllegalArgumentException("Rol desconegut: " + args[0]);
        }
    }

    // Comanda per iniciar una JVM nova que executa aquesta mateixa classe.
    private static List<String> command() {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return List.of(java, "-cp", System.getProperty("java.class.path"),
                echo.class.getName(), "echo");
    }

    private static void runSupervisor() throws Exception {
        // stderr es mostra al terminal; stdout es llegeix en una tasca separada.
        Process p = new ProcessBuilder(command())
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
        System.out.println("SUPERVISOR pid=" + ProcessHandle.current().pid()
                + " fill=" + p.pid());
        try (ExecutorService io = Executors.newSingleThreadExecutor()) {
            // El lector consumeix stdout mentre el fill treballa.
            Future<?> stdout = io.submit(() -> {
                try {
                    p.getInputStream().transferTo(System.out);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            // Escriure al flux de sortida del pare alimenta stdin del fill.
            try (BufferedWriter writer = p.outputWriter(StandardCharsets.UTF_8)) {
                writer.write("Ebre\nprocessos\nFI\n");
            }
            // El tancament del writer envia EOF; FI és una marca del protocol.
            int exitCode = p.waitFor(); // Esperar el procés fill.
            stdout.get(); // Esperar que el lector acabi de recollir la sortida.
            System.out.println("ECHO exit=" + exitCode);
        }
    }

    private static void runEcho() throws IOException {
        ProcessHandle current = ProcessHandle.current();
        long ppid = current.parent().map(ProcessHandle::pid).orElse(-1L);
        System.err.printf("[ECHO pid=%d ppid=%d thread=%s] inici%n",
                current.pid(), ppid, Thread.currentThread().getName());
        try (BufferedReader reader = new BufferedReader(
                new java.io.InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            // Finalitza quan arriba FI o quan readLine retorna null (EOF).
            while ((line = reader.readLine()) != null && !line.equals("FI")) {
                System.out.println(line.toUpperCase(Locale.ROOT));
            }
        }
    }
}