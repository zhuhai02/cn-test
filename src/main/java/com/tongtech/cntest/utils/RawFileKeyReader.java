package com.tongtech.cntest.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import com.tongtech.cnmq.client.api.CryptoKeyReader;
import com.tongtech.cnmq.client.api.EncryptionKeyInfo;
import com.tongtech.cnmq.client.api.CnmqClientException;

public class RawFileKeyReader implements CryptoKeyReader {
    String publicKeyFile = "";
    String privateKeyFile = "";
    public RawFileKeyReader(String pubKeyFile, String privKeyFile) throws CnmqClientException {
        publicKeyFile = pubKeyFile;
        privateKeyFile = privKeyFile;
    }
    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        try {
            keyInfo.setKey(Files.readAllBytes(Paths.get(publicKeyFile)));
        } catch (IOException e) {
            System.out.println("ERROR: Failed to read public key from file " + publicKeyFile);
            e.printStackTrace();
        }
        return keyInfo;
    }
    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        try {
            keyInfo.setKey(Files.readAllBytes(Paths.get(privateKeyFile)));
        } catch (IOException e) {
            System.out.println("ERROR: Failed to read private key from file " + privateKeyFile);
            e.printStackTrace();
        }
        return keyInfo;
    }
}
