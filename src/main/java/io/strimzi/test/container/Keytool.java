/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * Fluent wrapper for keytool operations executed inside a container.
 * Each factory method returns a command builder with named setters
 * and a terminal {@code execute()} method, keeping the certificate
 * orchestration in {@link CertAssembly} separate from the tool
 * invocation details.
 */
class Keytool {

    private final StrimziKafkaContainer container;

    Keytool(StrimziKafkaContainer container) {
        this.container = container;
    }

    /**
     * Starts building a command that generates a public/private
     * key pair in a PKCS12 keystore.
     */
    GenerateKeyPairCommand generateKeyPair() {
        return new GenerateKeyPairCommand();
    }

    /**
     * Starts building a command that exports a certificate
     * from a keystore to a file.
     */
    ExportCertificateCommand exportCertificate() {
        return new ExportCertificateCommand();
    }

    /**
     * Starts building a command that creates a Certificate
     * Signing Request (CSR) from a keystore entry.
     */
    GenerateCsrCommand generateCertificateSigningRequest() {
        return new GenerateCsrCommand();
    }

    /**
     * Starts building a command that signs a CSR using
     * a CA's private key.
     */
    SignCertificateCommand signCertificateRequest() {
        return new SignCertificateCommand();
    }

    /**
     * Starts building a command that imports a trusted CA
     * certificate into a keystore or truststore.
     */
    ImportTrustedCertificateCommand importTrustedCertificate() {
        return new ImportTrustedCertificateCommand();
    }

    /**
     * Starts building a command that imports a CA-signed
     * certificate back into its keystore, completing
     * the certificate chain.
     */
    ImportSignedCertificateCommand importSignedCertificate() {
        return new ImportSignedCertificateCommand();
    }

    /**
     * Generates a public/private key pair in a PKCS12 keystore.
     * Call {@link #asCa()} to add the basic-constraints CA extension.
     */
    class GenerateKeyPairCommand {
        private String keystore;
        private String password;
        private String alias;
        private String dname;
        private boolean isCa;

        GenerateKeyPairCommand keystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        GenerateKeyPairCommand password(String password) {
            this.password = password;
            return this;
        }

        GenerateKeyPairCommand alias(String alias) {
            this.alias = alias;
            return this;
        }

        GenerateKeyPairCommand dname(String dname) {
            this.dname = dname;
            return this;
        }

        /**
         * Marks this key pair as a Certificate Authority
         * by adding {@code bc:c=ca:true}.
         */
        GenerateKeyPairCommand asCa() {
            this.isCa = true;
            return this;
        }

        void execute() throws IOException, InterruptedException {
            if (isCa) {
                execKeytool("genkeypair (" + alias + ")",
                    "keytool", "-genkeypair",
                    "-keystore", keystore,
                    "-storetype", "PKCS12",
                    "-storepass", password,
                    "-keypass", password,
                    "-alias", alias,
                    "-keyalg", "RSA",
                    "-keysize", "2048",
                    "-validity", "365",
                    "-dname", dname,
                    "-ext", "bc:c=ca:true");
            } else {
                execKeytool("genkeypair (" + alias + ")",
                    "keytool", "-genkeypair",
                    "-keystore", keystore,
                    "-storetype", "PKCS12",
                    "-storepass", password,
                    "-keypass", password,
                    "-alias", alias,
                    "-keyalg", "RSA",
                    "-keysize", "2048",
                    "-validity", "365",
                    "-dname", dname);
            }
        }
    }

    /**
     * Exports a certificate from a keystore to a file.
     */
    class ExportCertificateCommand {
        private String keystore;
        private String password;
        private String alias;
        private String toFile;

        ExportCertificateCommand keystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        ExportCertificateCommand password(String password) {
            this.password = password;
            return this;
        }

        ExportCertificateCommand alias(String alias) {
            this.alias = alias;
            return this;
        }

        ExportCertificateCommand toFile(String toFile) {
            this.toFile = toFile;
            return this;
        }

        void execute() throws IOException, InterruptedException {
            execKeytool("exportcert (" + alias + ")",
                "keytool", "-exportcert",
                "-keystore", keystore,
                "-storepass", password,
                "-alias", alias,
                "-file", toFile);
        }
    }

    /**
     * Creates a Certificate Signing Request (CSR) from a keystore entry.
     */
    class GenerateCsrCommand {
        private String keystore;
        private String password;
        private String alias;
        private String toFile;

        GenerateCsrCommand keystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        GenerateCsrCommand password(String password) {
            this.password = password;
            return this;
        }

        GenerateCsrCommand alias(String alias) {
            this.alias = alias;
            return this;
        }

        GenerateCsrCommand toFile(String toFile) {
            this.toFile = toFile;
            return this;
        }

        void execute() throws IOException, InterruptedException {
            execKeytool("certreq (" + alias + ")",
                "keytool", "-certreq",
                "-keystore", keystore,
                "-storepass", password,
                "-alias", alias,
                "-file", toFile);
        }
    }

    /**
     * Signs a Certificate Signing Request using a CA's private key.
     * Optionally applies Subject Alternative Names (SANs) via
     * {@link #withSans(String)}.
     */
    class SignCertificateCommand {
        private String caKeystore;
        private String caPassword;
        private String csrFile;
        private String signedCertFile;
        private String sanExtension;

        SignCertificateCommand caKeystore(String caKeystore) {
            this.caKeystore = caKeystore;
            return this;
        }

        SignCertificateCommand caPassword(String caPassword) {
            this.caPassword = caPassword;
            return this;
        }

        SignCertificateCommand csrFile(String csrFile) {
            this.csrFile = csrFile;
            return this;
        }

        SignCertificateCommand signedCertFile(String signedCertFile) {
            this.signedCertFile = signedCertFile;
            return this;
        }

        SignCertificateCommand withSans(String sanExtension) {
            this.sanExtension = sanExtension;
            return this;
        }

        void execute() throws IOException, InterruptedException {
            if (sanExtension != null) {
                execKeytool("gencert (sign CSR)",
                    "keytool", "-gencert",
                    "-keystore", caKeystore,
                    "-storepass", caPassword,
                    "-alias", "ca",
                    "-infile", csrFile,
                    "-outfile", signedCertFile,
                    "-validity", "365",
                    "-ext", sanExtension);
            } else {
                execKeytool("gencert (sign CSR)",
                    "keytool", "-gencert",
                    "-keystore", caKeystore,
                    "-storepass", caPassword,
                    "-alias", "ca",
                    "-infile", csrFile,
                    "-outfile", signedCertFile,
                    "-validity", "365");
            }
        }
    }

    /**
     * Imports a trusted CA certificate into a keystore or truststore.
     * Uses {@code -noprompt} to accept the untrusted certificate.
     */
    class ImportTrustedCertificateCommand {
        private String keystore;
        private String password;
        private String alias;
        private String fromFile;

        ImportTrustedCertificateCommand keystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        ImportTrustedCertificateCommand password(String password) {
            this.password = password;
            return this;
        }

        ImportTrustedCertificateCommand alias(String alias) {
            this.alias = alias;
            return this;
        }

        ImportTrustedCertificateCommand fromFile(String fromFile) {
            this.fromFile = fromFile;
            return this;
        }

        void execute() throws IOException, InterruptedException {
            execKeytool("importcert (" + alias + " -> " + keystore + ")",
                "keytool", "-importcert",
                "-keystore", keystore,
                "-storetype", "PKCS12",
                "-storepass", password,
                "-alias", alias,
                "-file", fromFile,
                "-noprompt");
        }
    }

    /**
     * Imports a CA-signed certificate back into its keystore,
     * completing the certificate chain.
     */
    class ImportSignedCertificateCommand {
        private String keystore;
        private String password;
        private String alias;
        private String fromFile;

        ImportSignedCertificateCommand keystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        ImportSignedCertificateCommand password(String password) {
            this.password = password;
            return this;
        }

        ImportSignedCertificateCommand alias(String alias) {
            this.alias = alias;
            return this;
        }

        ImportSignedCertificateCommand fromFile(String fromFile) {
            this.fromFile = fromFile;
            return this;
        }

        void execute() throws IOException, InterruptedException {
            execKeytool("importcert (signed " + alias + ")",
                "keytool", "-importcert",
                "-keystore", keystore,
                "-storepass", password,
                "-alias", alias,
                "-file", fromFile);
        }
    }

    /**
     * Executes a keytool command inside the container and throws on failure.
     */
    private void execKeytool(String stepName, String... command)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.execInContainer(command);
        if (result.getExitCode() != 0) {
            throw new RuntimeException("keytool " + stepName
                + " failed (exit code " + result.getExitCode()
                + "): stdout=" + result.getStdout()
                + " stderr=" + result.getStderr());
        }
    }
}