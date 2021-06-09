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
    private JButton addButton;
    private JTextArea fileListTextArea;
    private JTextField hdfsFileIndex;
    private JButton averageButton;
    private JButton medianButton;
    private JButton standardDeviationButton;
    private boolean check;
    private JFileChooser fileChooser;
    private ArrayList<String> hdfsFilesArr;
    private ArrayList<String> hdfsOutputFilesArr;


    public MainScreen(){
        formHeaderText.setFont(new Font("Serif", Font.PLAIN, 20));
        check = false;
        fileChooser = new JFileChooser();
        hdfsFilesArr = new ArrayList<>();
        hdfsOutputFilesArr = new ArrayList<>();

        hdfsOutputFilesArr = getOutputFiles();
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

        averageButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try {
                    int index = Integer.parseInt(hdfsFileIndex.getText());

                    String[] splitted = hdfsFilesArr.get(index - 1).split("/");
                    String lastStr = splitted[splitted.length - 1];
                    System.out.println(lastStr);
                    String[] splitted2 = lastStr.split("\\.");
                    System.out.println(splitted2.length);
                    String coreStr = splitted2[0];

                    String command0 = "mkdir /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/average/" + coreStr;
                    Runtime.getRuntime().exec(command0);
                    System.out.println("Command0: " + command0);


                    String command1 = "hadoop jar /home/hadoopuser/hadoop-local/MapReduceFolder/class_files/WordMean/WordMean.jar WordMean /user/hadoopuser/WordCountDeneme1/input/" + lastStr + " /user/hadoopuser/WordCountDeneme1/output/" + coreStr;
                    String command2 = "hadoop fs -copyToLocal /user/hadoopuser/WordCountDeneme1/output/" + coreStr +  "/*" + " /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/average/" + coreStr;

                    Process process = Runtime.getRuntime().exec(command1);
                    BufferedReader breader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String output;

                    System.out.println(command1);
                    while ((output = breader.readLine()) != null) {
                        System.out.println(output);
                    }
                    System.out.println(command2);
                    Runtime.getRuntime().exec(command2);

                    JOptionPane.showMessageDialog(null, "Dosya başarıyla hdfs'ye kaydedildi.");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        });

        medianButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try {
                    int index = Integer.parseInt(hdfsFileIndex.getText());

                    String[] splitted = hdfsFilesArr.get(index - 1).split("/");
                    String lastStr = splitted[splitted.length - 1];
                    System.out.println(lastStr);
                    String[] splitted2 = lastStr.split("\\.");
                    System.out.println(splitted2.length);
                    String coreStr = splitted2[0];

                    String command0 = "mkdir /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/median/" + coreStr;
                    Runtime.getRuntime().exec(command0);
                    System.out.println("Command0: " + command0);


                    String command1 = "hadoop jar /home/hadoopuser/hadoop-local/MapReduceFolder/class_files/WordMedian/WordMedianJar.jar WordMedian /user/hadoopuser/WordCountDeneme1/input/" + lastStr + " /user/hadoopuser/WordCountDeneme1/output/" + coreStr;
                    String command2 = "hadoop fs -copyToLocal /user/hadoopuser/WordCountDeneme1/output/" + coreStr +  "/*" + " /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/median/" + coreStr;

                    Process process = Runtime.getRuntime().exec(command1);
                    BufferedReader breader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String output;

                    System.out.println(command1);
                    while ((output = breader.readLine()) != null) {
                        System.out.println(output);
                    }
                    System.out.println(command2);
                    Runtime.getRuntime().exec(command2);

                    JOptionPane.showMessageDialog(null, "Dosya başarıyla hdfs'ye kaydedildi.");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        });

        standardDeviationButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try {
                    int index = Integer.parseInt(hdfsFileIndex.getText());

                    String[] splitted = hdfsFilesArr.get(index - 1).split("/");
                    String lastStr = splitted[splitted.length - 1];
                    System.out.println(lastStr);
                    String[] splitted2 = lastStr.split("\\.");
                    System.out.println(splitted2.length);
                    String coreStr = splitted2[0];

                    String command0 = "mkdir /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/SD/" + coreStr;
                    Runtime.getRuntime().exec(command0);
                    System.out.println("Command0: " + command0);


                    String command1 = "hadoop jar /home/hadoopuser/hadoop-local/MapReduceFolder/class_files/WordSD/WordSD.jar WordStandardDeviation /user/hadoopuser/WordCountDeneme1/input/" + lastStr + " /user/hadoopuser/WordCountDeneme1/output/" + coreStr;
                    String command2 = "hadoop fs -copyToLocal /user/hadoopuser/WordCountDeneme1/output/" + coreStr +  "/*" + " /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/SD/" + coreStr;

                    Process process = Runtime.getRuntime().exec(command1);
                    BufferedReader breader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String output;

                    System.out.println(command1);
                    while ((output = breader.readLine()) != null) {
                        System.out.println(output);
                    }
                    System.out.println(command2);
                    Runtime.getRuntime().exec(command2);

                    JOptionPane.showMessageDialog(null, "Dosya başarıyla hdfs'ye kaydedildi.");
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
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

    private ArrayList<String> getOutputFiles(){
        ArrayList<String> hdfsOutputArr = new ArrayList<>();
        try {
            String command = "hadoop fs -ls /user/hadoopuser/WordCountDeneme1/output";
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
                hdfsOutputArr.add(filePath);

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        String fileListText = "";
        for (int i = 0; i < hdfsOutputArr.size(); i++) {
            int fileIndex = i + 1;
            fileListText += fileIndex + "-) " + hdfsOutputArr.get(i) + "\n";
        }


        return hdfsOutputArr;
    }


    private void copyHdfsFiles(int index){
        index -= 1;
        try {
            String[] splitted = hdfsFilesArr.get(index).split("/");

            System.out.println("Gecti");

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

    private void copyOutputHdfsFiles(int index){
        index -= 1;
        try {
            String[] splitted = hdfsOutputFilesArr.get(index).split("/");
            String lastStr = splitted[splitted.length - 1];
            System.out.println(lastStr);

            //String coreStr = lastStr.split(".")[0];

            String command0 = "mkdir /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/" + lastStr;
            Runtime.getRuntime().exec(command0);
            System.out.println("Command0: " + command0);

            System.out.println("Gecti");

            String command = "hadoop fs -copyToLocal " + hdfsOutputFilesArr.get(index) + " /home/hadoopuser/hadoop-local/MapReduceFolder/copiedOutputs/" + lastStr;
            System.out.println("Command: " + command);
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
        System.out.println(System.getProperty("user.name"));
        System.setProperty("user.name", "hadoopuser");
        System.out.println(System.getProperty("user.name"));

        JFrame jf = new JFrame("Big Data");
        jf.setContentPane(new MainScreen().panel1);
        jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        jf.pack();
        jf.setVisible(true);
        jf.setSize(500, 500);
        jf.setLocationRelativeTo(null);
    }

}