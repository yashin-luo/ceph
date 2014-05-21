/*
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package com.ceph.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class CephNativeLoader {

  private static String LIB_BASE_NAME = "cephfs_jni";
  private static String LIB_SUFFIX = ".so";
  private static String LIB_JAR_PATH_PREFIX = "/com/ceph/native/";
  private static String TEMP_FILE_PREFIX = "libcephfs_jni";
  private static String LIBPATH_PROPERTY = "cephfs.jni.library";
  private static String LIBPATH_ENV = "CEPHFS_JNI_LIBRARY";

  private static boolean loaded = false;

  /*
   * Build system prefix string (osname-arch). Supports:
   *   - Linux
   *   - x86-64
   *   - x86 (32-bit)
   */
  private static String getSystemPrefix() {
    String arch = System.getProperty("os.arch");
    if (arch.equals("i386")) {
      arch = "x86";
    } else if (arch.equals("x86_64") || arch.equals("amd64")) {
      arch = "x86-64";
    }

    // Only support Linux right now.
    String os = "linux";

    return os + "-" + arch;
  }

  /*
   * Try to load the JNI library from the jar package.
   */
  private static void loadLibraryFromJar(String sysPrefix) throws IOException {
    /*
     * Build library path.
     *   - Example: /com/ceph/native/linux-x86-64/libcephfs_jni.so
     */
    String libpath = LIB_JAR_PATH_PREFIX + sysPrefix +
      "/" + LIB_BASE_NAME + LIB_SUFFIX;

    // stream from which we will read the embedded library
    InputStream istream = CephNativeLoader.class.getResourceAsStream(libpath);
    if (istream == null) {
      throw new FileNotFoundException(libpath);
    }

    // where the extracted binary will live
    File tempFile = File.createTempFile(TEMP_FILE_PREFIX, null);
    tempFile.deleteOnExit();

    // copy the embedded library into the temp file
    OutputStream ostream = new FileOutputStream(tempFile);
    try {
      int count;
      byte[] buf = new byte[4096];
      while ((count = istream.read(buf)) != -1) {
        ostream.write(buf, 0, count);
      }
    } catch (Exception e) {
    } finally {
      istream.close();
      ostream.close();
    }

    // load the JNI shim library!
    System.load(tempFile.getAbsolutePath());
  }

  /*
   * Load and initialize the CephFS JNI library.
   *
   * Precedence order:
   *   - cephfs.jni.library (Java property)
   *   - CEPHFS_JNI_LIBRARY (Environment var)
   *   - Default paths (java.library.path)
   *   - Embedded in jar package
   */
  static {
    if (!loaded) {
      // get path specified in property or env variable
      String libpath = System.getProperty(LIBPATH_PROPERTY);
      if (libpath == null)
        libpath = System.getenv(LIBPATH_ENV);

      if (libpath != null) {
        System.load(libpath);
      } else {
        try {
          // search default paths (i.e. java.library.path)
          System.loadLibrary(LIB_BASE_NAME);
        } catch (java.lang.UnsatisfiedLinkError e1) {
          // load from library embedded in jar package
          String sysPrefix = getSystemPrefix();
          try {
            loadLibraryFromJar(sysPrefix);
          } catch (IOException e2) {
            e2.printStackTrace();
            throw new RuntimeException(
                "Error loading CephFS JNI library (" + sysPrefix + ")");
          }
        }
      }

      CephMount.native_initialize();
      loaded = true;
    }
  }

  static void checkLoaded() { assert(loaded); }
}
