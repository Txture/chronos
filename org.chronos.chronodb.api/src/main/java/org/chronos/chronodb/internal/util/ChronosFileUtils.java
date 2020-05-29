package org.chronos.chronodb.internal.util;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.chronos.common.logging.ChronoLogger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static com.google.common.base.Preconditions.*;

public class ChronosFileUtils {

    private static final int UNZIP_BUFFER_SIZE_BYTES = 4096;

    private ChronosFileUtils() {
        throw new UnsupportedOperationException("Do not instantiate this class!");
    }

    public static boolean isGZipped(final File file) {
        // code taken from: http://stackoverflow.com/a/30507742/3094906
        int magic = 0;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            magic = raf.read() & 0xff | raf.read() << 8 & 0xff00;
        } catch (Exception e) {
            ChronoLogger.logError("Failed to read file '" + file.getAbsolutePath() + "'!", e);
        }
        return magic == GZIPInputStream.GZIP_MAGIC;
    }

    public static boolean isExistingFile(final File file) {
        if (file == null) {
            return false;
        }
        if (file.exists() == false) {
            return false;
        }
        if (file.isFile() == false) {
            return false;
        }
        return true;
    }

    /**
     * Extracts the given zip file into the given target directory.
     *
     * @param zipFile         The *.zip file to extract. Must not be <code>null</code>, must refer to an existing file.
     * @param targetDirectory The target directory to extract the data to. Must not be <code>null</code>, must be an existing directory.
     * @throws IOException Thrown if an I/O error occurs during the process.
     */
    public static void extractZipFile(final File zipFile, final File targetDirectory) throws IOException {
        checkNotNull(zipFile, "Precondition violation - argument 'zipFile' must not be NULL!");
        checkNotNull(targetDirectory, "Precondition violation - argument 'targetDirectory' must not be NULL!");
        checkArgument(zipFile.exists(), "Precondition violation - argument 'zipFile' must refer to an existing file!");
        checkArgument(targetDirectory.exists(), "Precondition violation - argument 'targetDirectory' must refer to an existing file!");
        checkArgument(zipFile.isFile(), "Precondition violation - argument 'zipFile' must point to a file (not a directory)!");
        checkArgument(targetDirectory.isDirectory(), "Precondition violation - argument 'targetDirectory' must point to a directory (not a file)!");
        try (ZipInputStream zin = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            String name;
            String dir;
            while ((entry = zin.getNextEntry()) != null) {
                name = entry.getName();
                if (entry.isDirectory()) {
                    mkdirs(targetDirectory, name);
                    continue;
                }
                /*
                 * this part is necessary because file entry can come before directory entry where is file located i.e.: /foo/foo.txt /foo/
                 */
                dir = dirpart(name);
                if (dir != null) {
                    mkdirs(targetDirectory, dir);
                }

                extractFile(zin, targetDirectory, name);
            }
        }
    }

    private static void extractFile(final ZipInputStream in, final File outdir, final String name) throws IOException {
        byte[] buffer = new byte[UNZIP_BUFFER_SIZE_BYTES];
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(outdir, name)))) {
            int count = -1;
            while ((count = in.read(buffer)) != -1) {
                out.write(buffer, 0, count);
            }
        }
    }

    private static void mkdirs(final File outdir, final String path) {
        File d = new File(outdir, path);
        if (!d.exists()) {
            d.mkdirs();
        }
    }

    private static String dirpart(final String name) {
        int s = name.lastIndexOf(File.separatorChar);
        return s == -1 ? null : name.substring(0, s);
    }

    /**
     * Zips the given file and stores the result in the given target file.
     *
     * @param fileToZip     The file to zip. Must not be <code>null</code>. Must exist. May be a file or a directory. Zipping a directory will recursively zip all contents.
     * @param targetZipFile The target zip file to hold the compressed contents. Must not be <code>null</code>, must not be a directory. Will be created if it doesn't exist. Existing files will be overwritten.
     */
    public static void createZipFile(File fileToZip, File targetZipFile) throws IOException {
        checkNotNull(fileToZip, "Precondition violation - argument 'fileToZip' must not be NULL!");
        checkArgument(fileToZip.exists(), "Precondition violation - argument 'fileToZip' must exist!");
        checkNotNull(targetZipFile, "Precondition violation - argument 'targetZipFile' must not be NULL!");
        if (targetZipFile.exists()) {
            checkArgument(targetZipFile.isFile(), "Precondition violation - argument 'targetZipFile' must not be a directory!");
            Files.delete(targetZipFile.toPath());
        }
        Files.createFile(targetZipFile.toPath());
        try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(targetZipFile))) {
            zipFile(fileToZip, fileToZip.getName(), zipOut);
        }
    }

    private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith("/")) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            for (File childFile : children) {
                zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
            }
        } else {
            try (FileInputStream fis = new FileInputStream(fileToZip)) {
                ZipEntry zipEntry = new ZipEntry(fileName);
                zipOut.putNextEntry(zipEntry);
                byte[] bytes = new byte[1024];
                int length;
                while ((length = fis.read(bytes)) >= 0) {
                    zipOut.write(bytes, 0, length);
                }
            }
        }
    }

    /**
     * Calculates the sha256 hash of the given file or directory
     *
     * @param file             a folder or file
     * @return the calculated sha256 hash
     */
    public static HashCode sha256OfContent(File file) {
        checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
        return sha256Internal(file, null);
    }

    /**
     * Calculates the sha256 hash of the given file or directory
     *
     * @param file             a folder or file
     * @param pathPrefix        the prefix to ignore when calculating the hash for filepaths
     * @return the calculated sha256 hash
     */
    public static HashCode sha256OfContentAndFileName(File file, String pathPrefix) {
        checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
        checkNotNull(pathPrefix, "Precondition violation - argument 'pathPrefix' must not be NULL!");
        return sha256Internal(file, pathPrefix);
    }


    private static HashCode sha256Internal(File file, String pathPrefix){
        checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
        checkArgument(file.exists(), "Precondition violation - argument 'file' must exist!");
        try {
            if(file.isFile()){
                HashCode hash = getContentHashCode(file);
                if(pathPrefix != null){
                    HashCode pathHash = getPathHashCode(file, pathPrefix);
                    return Hashing.combineUnordered(Lists.newArrayList(hash, pathHash));
                }else{
                    return hash;
                }
            } else {
                Iterable<File> files = com.google.common.io.Files.fileTraverser()
                    .depthFirstPostOrder(file);
                List<HashCode> hashCodes = Streams.stream(files).map(f -> {
                    if(f.isFile()){
                        return sha256Internal(f, pathPrefix);
                    }else{
                        return getPathHashCode(f, pathPrefix);
                    }
                }).collect(Collectors.toList());
                return Hashing.combineUnordered(hashCodes);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("The given file \"" + file.getName() + "\" is not an archive and thus can not be extracted");
        }
    }

    private static HashCode getContentHashCode(final File file) throws IOException {
        return com.google.common.io.Files.asByteSource(file).hash(Hashing.sha256());
    }

    private static HashCode getPathHashCode(final File file, final String pathPrefix) {
        String absolutePath = file.getAbsolutePath();
        String path;
        if(absolutePath.startsWith(pathPrefix)){
            path = absolutePath.substring(pathPrefix.length());
        }else{
            path = absolutePath;
        }
        return Hashing.sha256().hashString(path.replaceAll("[\\\\/]", ""), Charsets.UTF_8);
    }
}
