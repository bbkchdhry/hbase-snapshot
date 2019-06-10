import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

public class TarGZIP {

    /**
     *
     * @param source
     */
    public void createTarFile(String source){
        TarArchiveOutputStream tarOs = null;
        try {
            FileOutputStream fos = new FileOutputStream("/home/saque/hbase-snapshots/hbase-snapshot.tar.gz");
            GZIPOutputStream gos = new GZIPOutputStream(new BufferedOutputStream(fos));
            tarOs = new TarArchiveOutputStream(gos);
            tarOs.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            File folder = new File(source);
            File[] fileNames = folder.listFiles();
            for(File file : fileNames){
                System.out.println("PATH " + file.getAbsolutePath());
                System.out.println("File name " + file.getName());
                addFilesToTarGZ(file.getAbsolutePath(), file, tarOs);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                tarOs.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    /**
     *
     * @param source
     * @param file
     * @param tos
     * @throws IOException
     */
    public void addFilesToTarGZ(String source, File file, TarArchiveOutputStream tos)
            throws IOException{
        // New TarArchiveEntry
        tos.putArchiveEntry(new TarArchiveEntry(file, source));
        if(file.isFile()){
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis);
            // Write content of the file
            IOUtils.copy(bis, tos);
            tos.closeArchiveEntry();
            fis.close();
        }else if(file.isDirectory()){
            // no need to copy any content since it is
            // a directory, just close the outputstream
            tos.closeArchiveEntry();
            for(File cFile : file.listFiles()){
                // recursively call the method for all the subfolders
                addFilesToTarGZ(cFile.getAbsolutePath(), cFile, tos);

            }
        }

    }
}