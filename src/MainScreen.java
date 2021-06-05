import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class MainScreen {
    private JPanel panel1;
    private JLabel formHeaderText;
    private JButton getButton;
    private JLabel emptyLabel;
    private JButton addButton;
    private JTextArea fileListTextArea;
    private JTextField hdfsFileIndex;
    private boolean check;
    private JFileChooser fileChooser;
    private ArrayList<String> hdfsFilesArr;


    public MainScreen(){
        formHeaderText.setFont(new Font("Serif", Font.PLAIN, 20));
        check = false;
        fileChooser = new JFileChooser();
        hdfsFilesArr = new ArrayList<>();
        hdfsFilesArr = getHdfsFiles();


        getButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(ActionEvent e) {

                try{
                    int index = Integer.parseInt(hdfsFileIndex.getText());
                    copyHdfsFiles(index);
                    JOptionPane.showMessageDialog(null, "Dosya basariyla local diske kopyalandı.");

                }catch(Exception ex){
                    JOptionPane.showMessageDialog(null, "Geçersiz dosya indeksi.");
                }

            }
        });

        addButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                addHdfsFiles();
            }
        });
    }


    public void addHdfsFiles(){
        String filePath;
        if (fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
            java.io.File f = fileChooser.getSelectedFile();
            filePath = f.getPath();
            System.out.println(filePath);
        }else{
            JOptionPane.showMessageDialog(null, "Dosya bulunamadi.");
            return;
        }


        try {
            String hdfsInputDirectory = " /user/hadoopuser/WordCountDeneme1/input";
            String command = "hadoop fs -put " + filePath + hdfsInputDirectory;
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader breader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output;

            System.out.println(command);
            while ((output = breader.readLine()) != null) {
                System.out.println(output);
            }
            hdfsFilesArr = getHdfsFiles();
            JOptionPane.showMessageDialog(null, "Dosya başarıyla hdfs'ye kaydedildi.");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        //hadoop fs -put /home/madcodder/Desktop/newtxt4.txt /user/hadoopuser/WordCountDeneme1/input



    }

    private ArrayList<String> getHdfsFiles(){
        ArrayList<String> hdfsInputArr = new ArrayList<>();
        try {
            String command = "hadoop fs -ls /user/hadoopuser/WordCountDeneme1/input";
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader breader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output;
            int counter = 0;

            while ((output = breader.readLine()) != null) {
                if(counter == 0){
                    counter ++;
                    continue;
                }
                String[] parsedOutput = output.split(" ");
                String filePath = parsedOutput[parsedOutput.length - 1];
                System.out.println(filePath);
                hdfsInputArr.add(filePath);

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        String fileListText = "";
        for (int i = 0; i < hdfsInputArr.size(); i++) {
            int fileIndex = i + 1;
            fileListText += fileIndex + "-) " + hdfsInputArr.get(i) + "\n";
        }

        fileListTextArea.setText(fileListText);
        return hdfsInputArr;
    }

    private void copyHdfsFiles(int index){
        index -= 1;
        try {
            String command = "hadoop fs -copyToLocal " + hdfsFilesArr.get(index) + " /home/hadoopuser/hadoop-local/";
            System.out.println(command);
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader breader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output;

            while ((output = breader.readLine()) != null) {
                System.out.println(output);

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }




    public static void main(String[] args) {
        JFrame jf = new JFrame("Ilk App");
        jf.setContentPane(new MainScreen().panel1);
        jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        jf.pack();
        jf.setVisible(true);
        jf.setSize(500, 500);
        jf.setLocationRelativeTo(null);
    }

}