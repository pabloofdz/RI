package es.udc.fi.ri.mrisearcher;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Compare {
    public Compare() {}
    public static void main(String[] args) throws IOException {
        String usage = "java es.udc.fi.ri.mrisearcher.Compare"
                + " -test t|wilcoxon alpha -results results1.csv results2.csv\n\n"
                + "(Test de significancia estadística -t-test o Wilcoxon- y nivel de significancia\n" +
                "alpha. 0 < alpha <= 0.5)\n" +
                "(results1 y results2 son archivos de resultados obtenidos con\n" +
                "TrainingTestNPL para la misma métrica y sobre las mismas queries de test)";
        // Process command line arguments
        String testType = "";
        double alpha = 0;
        String resultsFile1 = "";
        String resultsFile2 = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-test":
                    testType = args[++i];
                    alpha = Double.parseDouble(args[++i]);
                    break;
                case "-results":
                    resultsFile1 = args[++i];
                    resultsFile2 = args[++i];
                    break;
                default:
                    System.err.println("Invalid argument: " + args[i]);
                    System.exit(1);
            }
        }

        if(testType.equals("") || alpha<=0 || alpha>0.5 || resultsFile1.equals("") || resultsFile2.equals("")){
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        int index1 = resultsFile1.indexOf(".test.");
        int index2 = resultsFile2.indexOf(".test.");

        if (index1 != -1 && index2 != -1) {  // Se han encontrado ambas cadenas en ambos nombres de archivo
            String subStr1 = resultsFile1.substring(index1 + 6, resultsFile1.indexOf(".test.csv"));
            String subStr2 = resultsFile2.substring(index2 + 6, resultsFile2.indexOf(".test.csv"));

            if (!subStr1.equals(subStr2)) {
                System.err.println("Los archivos de resultados no fueron obtenidos para la misma métrica y/o sobre las mismas queries de test");
                System.exit(1);
            }
        } else {
            System.err.println("Nombre de archivo no válido");
            System.exit(1);
        }

        // Leer los resultados de los archivos .csv
        double[] results1 = readResultsFromFile(resultsFile1);
        double[] results2 = readResultsFromFile(resultsFile2);

        // Realizar el test de significancia estadística
        double pValue = 0;
        if (testType.equals("t")) {
            TTest tTest = new TTest();
            pValue = tTest.pairedTTest(results1, results2);
        } else if(testType.equals("wilcoxon")){
            WilcoxonSignedRankTest wilcoxon = new WilcoxonSignedRankTest();
            pValue = wilcoxon.wilcoxonSignedRankTest(results1, results2, false);
        } else{
            System.err.println("Invalid test type: " + testType);
            System.exit(1);
        }

        // Imprimir el resultado del test y el p-valor comparando con alpha
        System.out.println("Test: " + testType);
        System.out.println("Alpha: " + alpha);
        if(pValue < alpha){
            System.out.println("Resultado del test: Hay evidencia estadística para rechazar la hipótesis nula.");
        } else{
            System.out.println("Resultado del test: No hay suficiente evidencia estadística para rechazar la hipótesis nula.");
        }
        System.out.println("P-value: " + pValue);
    }


    private static double[] readResultsFromFile(String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        ArrayList<Double> resultsList = new ArrayList<>();
        boolean firstLineSkipped = false;
        while ((line = reader.readLine()) != null) {
            if (!firstLineSkipped) {
                firstLineSkipped = true;
            } else if (line.startsWith("Promedio")) {
                break;  // Salir del bucle al encontrar la última línea
            } else {
                String[] parts = line.split(",");
                double result = Double.parseDouble(parts[1]);
                resultsList.add(result);
            }
        }
        reader.close();
        double[] results = new double[resultsList.size()];
        for (int i = 0; i < resultsList.size(); i++) {
            results[i] = resultsList.get(i);
        }
        return results;
    }

}
