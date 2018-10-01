import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class CreateDeleteMoveDirectory {

    public void createNewDirectory(File directoryToCreate){

        if (!directoryToCreate.exists()) {
            if (directoryToCreate.mkdir()) {
                System.out.println(directoryToCreate.getName() + " Folder created successfully");
            } else {
                System.out.println("Failed to create Folder");
            }
        }else{
            System.out.println(directoryToCreate.getName() + " Folder already exists");
        }
    }

    public void deleteDirectory(File directoryToDelete){
        try {
            if(directoryToDelete.exists()){
                FileUtils.deleteDirectory(directoryToDelete);
                System.out.println("Folder Deleted Successfully");
            }else{
                System.out.println("Folder doesn't exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteContentsOfDirectory(File folderName){
        try {
            if(folderName.exists()){
                FileUtils.cleanDirectory(folderName);
                System.out.println("Contents of folder deleted successfully");
            }else{
                System.out.println("Folder doesn't exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
