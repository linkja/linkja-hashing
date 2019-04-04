package org.linkja.hashing;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.jcajce.provider.symmetric.AES;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 *
 * Credit to the following resources for education, inspiration as well as code used and adapted within this class.
 *   https://github.com/1MansiS/java_crypto/blob/master/cipher/SecuredGCMUsage.java
 *   https://stackoverflow.com/a/46828430
 *   https://stackoverflow.com/a/29532412
 *   https://stackoverflow.com/q/42501609
 *
 * Changes/refactoring were done to all of the code acknowledged that we were not able to use them directly, which
 * is why we don't have direct inclusion/attribution of those source files.
 */
public class CryptoHelper {
  private static String SALT_FILE_DELIMITER = ",";
  private static int NUM_SALT_PARTS = 5;
  private static int AES_KEY_SIZE_BITS = 256;
  private static int AES_KEY_SIZE_BYTES = AES_KEY_SIZE_BITS / 8;
  private static int IV_SIZE = 64;
  private static int TAG_BIT_LENGTH = 128;
  private static String ALGO_TRANSFORMATION_STRING = "AES/GCM/PKCS5Padding";
  private static int CIPHER_OUTPUT_BUFFER_SIZE = 8192;

  private static final byte[] AAD_DATA = "org.linkja.hashing.CryptoHelper".getBytes();

  public class AESParameters {
    public GCMParameterSpec GCMParams = null;
    public SecretKey Key = null;
  }

  /**
   * Randomly generate the necessary parameters for calling AES encryption.  This uses a locally defined class to
   * @return
   * @throws NoSuchAlgorithmException
   */
  public AESParameters generateAESParameters() throws NoSuchAlgorithmException {
    // Generating Key
    SecretKey aesKey = null;
    KeyGenerator keygen = KeyGenerator.getInstance("AES") ; // Specifying algorithm key will be used for
    keygen.init(AES_KEY_SIZE_BITS); // Specifying Key size to be used, Note: This would need JCE Unlimited Strength to be installed explicitly
    aesKey = keygen.generateKey();

    // Generating IV
    byte iv[] = new byte[IV_SIZE];
    SecureRandom secRandom = new SecureRandom() ;
    secRandom.nextBytes(iv); // SecureRandom initialized using self-seeding

    // Initialize GCM Parameters
    GCMParameterSpec gcmParamSpec = new GCMParameterSpec(TAG_BIT_LENGTH, iv) ;

    AESParameters aesParameters = new AESParameters();
    aesParameters.Key = aesKey;
    aesParameters.GCMParams = gcmParamSpec;
    return aesParameters;
  }

  /**
   * Use a public RSA key to encrypt/sign the necessary parts of an AES key.  This is written as base64-encoded data
   * to a text file at the provided path.
   * @param publicKeyFile
   * @param aesParameters
   * @param outputFilePath
   * @throws Exception
   */
  public void rsaEncryptAES(File publicKeyFile, AESParameters aesParameters, Path outputFilePath) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(publicKeyFile));
    PEMParser parser = new PEMParser(reader);
    SubjectPublicKeyInfo spki = (SubjectPublicKeyInfo)parser.readObject();
    parser.close();
    reader.close();
    PublicKey publicKeyParam = new JcaPEMKeyConverter().getPublicKey(spki);

    byte[] unencryptedData = new byte[(AES_KEY_SIZE_BITS / 8) + IV_SIZE];
    System.arraycopy(aesParameters.Key.getEncoded(), 0, unencryptedData, 0, AES_KEY_SIZE_BYTES);
    System.arraycopy(aesParameters.GCMParams.getIV(), 0, unencryptedData, AES_KEY_SIZE_BYTES, IV_SIZE);

    Cipher encrypt = Cipher.getInstance("RSA/ECB/PKCS1Padding");
    encrypt.init(Cipher.PUBLIC_KEY, publicKeyParam);
    byte[] encryptedData = encrypt.doFinal(unencryptedData);
    Files.write(outputFilePath, Base64.getEncoder().encode(encryptedData));
  }

  /**
   * Perform AES-256 encryption on an input file, writing the encrypted binary data to disk as an output file
   * @param aesParameters
   * @param inputFile
   * @param outputFile
   * @throws BadPaddingException
   * @throws IllegalBlockSizeException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchPaddingException
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  public void encryptAES(AESParameters aesParameters, File inputFile, File outputFile) throws BadPaddingException, IllegalBlockSizeException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException, IOException {
    Cipher cipher = Cipher.getInstance(ALGO_TRANSFORMATION_STRING);
    cipher.init(Cipher.ENCRYPT_MODE, aesParameters.Key, aesParameters.GCMParams, new SecureRandom());
    cipher.updateAAD(AAD_DATA); // add AAD tag data before encrypting

    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile));
    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile));
    CipherOutputStream out = new CipherOutputStream(outputStream, cipher);
    byte[] buffer = new byte[CIPHER_OUTPUT_BUFFER_SIZE];
    int count;
    while ((count = inputStream.read(buffer)) > 0) {
      out.write(buffer, 0, count);
    }

    out.flush();
    out.close();
    outputStream.close();
    inputStream.close();
  }
  
  /**
   * Perform AES-256 decryption on an input file, writing the plaintext to disk as an output file.
   * @param aesParameters
   * @param inputFile
   * @param outputFile
   * @throws NoSuchPaddingException
   * @throws NoSuchAlgorithmException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws IOException
   */
  public void decryptAES(AESParameters aesParameters, File inputFile, File outputFile) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, IOException {
    Cipher cipher = Cipher.getInstance(ALGO_TRANSFORMATION_STRING); // Transformation specifies algortihm, mode of operation and padding
    cipher.init(Cipher.DECRYPT_MODE, aesParameters.Key, aesParameters.GCMParams, new SecureRandom()) ;
    cipher.updateAAD(AAD_DATA) ; // Add AAD details before decrypting

    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile));
    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile));
    CipherInputStream in = new CipherInputStream(inputStream, cipher);
    byte[] buffer = new byte[CIPHER_OUTPUT_BUFFER_SIZE];
    int count;
    while ((count = in.read(buffer)) >= 0) {
      outputStream.write(buffer, 0, count);
    }

    in.close();
    outputStream.flush();
    outputStream.close();
    inputStream.close();
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
    BufferedReader reader = new BufferedReader(new FileReader(decryptKey));
    PEMParser parser = new PEMParser(reader);
    PEMKeyPair pemKeyPair = (PEMKeyPair) parser.readObject();
    KeyPair keyPair = new JcaPEMKeyConverter().getKeyPair(pemKeyPair);
    parser.close();
    reader.close();

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
