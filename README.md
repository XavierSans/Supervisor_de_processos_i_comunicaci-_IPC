# Pràctica 01 — Supervisor de processos i comunicació IPC

## Descripció

Aquest projecte consisteix a desenvolupar una aplicació en Java que permet crear i supervisar processos externs, controlar-ne l'execució i gestionar la comunicació entre ells.

Durant la pràctica es treballa amb `ProcessBuilder`, la identificació dels processos mitjançant PID i PPID, la captura de l'entrada i l'eixida estàndard, la gestió d'errors i temps límit, i la comunicació entre processos mitjançant una pipe productor-consumidor.

També s'utilitza Maven per a compilar el projecte i `systemd` per a executar el supervisor com un servei en Linux.

## Tecnologies utilitzades

- Java 21
- Maven
- Git
- Linux i systemd

## Execució

Per a compilar el projecte, executa:

```bash
mvn clean compile
```

Per a iniciar el programa:

```bash
java -cp target/classes org.example.echo
```

## Enllaç a Google Drive

[Documentacio](https://docs.google.com/document/d/1VYzq68j0GZE1cRuiE9A1cDr8DlTyoW013sQB3ESfH2A/edit?tab=t.0)

## Autor

[Nom i cognoms]
