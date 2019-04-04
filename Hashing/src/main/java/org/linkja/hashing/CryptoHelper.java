package org.linkja.hashing;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.jcajce.provider.symmetric.AES;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class CryptoHelper {
  private static String SALT_FILE_DELIMITER = ",";
  private static int NUM_SALT_PARTS = 5;
  private static int AES_KEY_LENGTH = 256;

  public static KeyPair readPEMKey(File keyFile) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(keyFile));
    PEMParser parser = new PEMParser(reader);
    PEMKeyPair pemKeyPair = (PEMKeyPair) parser.readObject();
    KeyPair keyPair = new JcaPEMKeyConverter().getKeyPair(pemKeyPair);
    parser.close();
    reader.close();
    return keyPair;
  }

  // With many thanks to - https://stackoverflow.com/a/46828430/5670646
  public static void generateEncryptedKeyFile(File publicKeyFile, String outputFile) throws Exception {
    SecureRandom random = new SecureRandom();
    byte[] keyBytes = new byte[AES_KEY_LENGTH];
    random.nextBytes(keyBytes);
    SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
    //String encodedKey = Base64.getEncoder().encodeToString(key.getEncoded());

    BufferedReader reader = new BufferedReader(new FileReader(publicKeyFile));
    PEMParser parser = new PEMParser(reader);
    SubjectPublicKeyInfo spki = (SubjectPublicKeyInfo)parser.readObject();
    //AsymmetricKeyParameter publicKeyParam = PublicKeyFactory.createKey(spki);
    parser.close();
    reader.close();
    PublicKey publicKeyParam = new JcaPEMKeyConverter().getPublicKey(spki);

    //    X509EncodedKeySpec spec =
//            new X509EncodedKeySpec(publicKey.getEncoded());
//    KeyFactory kf = KeyFactory.getInstance("RSA/ECB/PKCS1Padding");
//    PublicKey key = kf.generatePublic(spec);
//
    Cipher encrypt = Cipher.getInstance("RSA/ECB/PKCS1Padding");
    encrypt.init(Cipher.PUBLIC_KEY, publicKeyParam);
    byte[] encryptedData = encrypt.doFinal(key.getEncoded());
    //String encryptedMessage = new String(encrypt.doFinal(publicKeyParam.getEncoded()), StandardCharsets.UTF_8);
  }

  /**
   * Given an encrypted salt file, and a private decryption key, get out the hashing parameters that include site and
   * project details.  Note that the HashParameters are considered sensitive information, because they are encrypted.
   * @param saltFile
   * @param decryptKey
   * @return
   * @throws Exception
   */
  public static HashParameters parseProjectSalt(File saltFile, File decryptKey, int minSaltLength) throws Exception {
    KeyPair keyPair = readPEMKey(decryptKey);

    Cipher decrypt = Cipher.getInstance("RSA/ECB/PKCS1Padding");
    decrypt.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
    String decryptedMessage = new String(decrypt.doFinal(Files.readAllBytes(saltFile.toPath())), StandardCharsets.UTF_8);
    String[] saltParts = decryptedMessage.split(SALT_FILE_DELIMITER);
    if (saltParts == null || saltParts.length < NUM_SALT_PARTS) {
      throw new LinkjaException("The salt file was not in the expected format.  Please confirm that you are referencing the correct file");
    }

    // At this point we have to assume that everything is in the right position, so we will load by position.
    HashParameters parameters = new HashParameters();
    parameters.setSiteId(saltParts[0]);
    parameters.setSiteName(saltParts[1]);
    parameters.setPrivateSalt(saltParts[2]);
    parameters.setProjectSalt(saltParts[3]);
    parameters.setProjectId(saltParts[4]);

    if (parameters.getProjectSalt().length() < minSaltLength) {
      throw new LinkjaException(String.format("The project salt must be at least %d characters long, but the one provided is %d",
              minSaltLength, parameters.getProjectSalt().length()));
    }
    if (parameters.getPrivateSalt().length() < minSaltLength) {
      throw new LinkjaException(String.format("The private (site-specific) salt must be at least %d characters long, but the one provided is %d",
              minSaltLength, parameters.getPrivateSalt().length()));
    }

    return parameters;
  }
}
