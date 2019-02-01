package org.linkja.Hashing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileHelper {
  public Path createDirectory(String directoryName) throws IOException {
    return Files.createDirectory(Paths.get(directoryName));
  }

  public boolean exists(File file) {
    return file.exists();
  }

  public boolean exists(Path path) {
    return Files.exists(path);
  }

  public Path pathFromString(String path) {
    return Paths.get(path);
  }
}
