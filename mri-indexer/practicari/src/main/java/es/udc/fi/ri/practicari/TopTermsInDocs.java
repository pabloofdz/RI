package es.udc.fi.ri.practicari;

import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class TopTermsInDocs {

    public static class TermWithValues {
        private final int docId;
        private final String term;
        private final int tf;
        private final int df;
        private final double idf;
        private final double tfidf;

        public TermWithValues(int docId, String term, int tf, int df, double idf, double tfidf) {
            this.docId = docId;
            this.term = term;
            this.tf = tf;
            this.df = df;
            this.idf = idf;
            this.tfidf = tfidf;
        }

        public int getDocId() {
            return docId;
        }

        public String getTerm() {
            return term;
        }

        public int getTf() {
            return tf;
        }

        public int getDf() {
            return df;
        }

        public double getIdf() {
            return idf;
        }

        public double getTfidf() {
            return tfidf;
        }
    }

    public static class TermComparator implements Comparator<TermWithValues> {

        @Override
        public int compare(TermWithValues term1, TermWithValues term2) {
            // Compara primero los docID
            int docIdCompare = Integer.compare(term1.getDocId(), term2.getDocId());
            if (docIdCompare != 0) {
                return docIdCompare;
            }
            // Si los docID son iguales, compara los valores de tfidf
            int tfidfCompare = Double.compare(term2.getTfidf(), term1.getTfidf());
            if (tfidfCompare != 0) {
                return tfidfCompare;
            }
            // Si los valores de tfidf son iguales, compara los términos
            return term1.getTerm().compareTo(term2.getTerm());
        }
    }

    public static final String CONTENT = "contents";

    public static void main(String[] args) throws Exception {
        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " -index INDEX_PATH -docID INT1-INT2 -top NUM -outfile PATH\n\n"
                        + "All integers must be greater than 0";
        String indexPath = null;
        int docID1 = -1;
        int docID2 = -1;
        int top = -1;
        String outPath = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docID":
                    String[] docIDs =  args[++i].split("-");
                    docID1 = Integer.parseInt(docIDs[0]);
                    docID2 = Integer.parseInt(docIDs[1]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outPath =  args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (indexPath == null || docID1 <= -1 || docID2 <= -1 || top <= -1 || outPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));

        Terms vector = null;
        TermsEnum termsEnum = null;
        ArrayList<TermWithValues> termsList= new ArrayList<>();
        BytesRef text = null;
        int numDocs = reader.numDocs();

        for (int i = docID1; i <= docID2; i++) {
            vector = reader.getTermVector(i, CONTENT);

            termsEnum = vector.iterator();
            while ((text = termsEnum.next()) != null) {
                String term = text.utf8ToString();
                // Obtener tf
                int freq = (int) termsEnum.totalTermFreq();

                // Calcular idf
                int docFreq = reader.docFreq(new Term(CONTENT, term));
                double idf = (double) numDocs / (double) docFreq;

                // Calcular raw tf * idflog10
                double tfIdf = freq * Math.log10(idf);

                TermWithValues termWithValues = new TermWithValues(i, term, freq, docFreq, idf, tfIdf);
                termsList.add(termWithValues);
            }
        }

        reader.close();

        termsList.sort(new TermComparator());


        // Crea un objeto PrintWriter para escribir en el archivo de salida
        PrintWriter writer = null;
        writer = new PrintWriter(new FileWriter(outPath));

        for (int i = docID1; i <= docID2; i++) {
            // Filtra la lista de términos para obtener solo los del documento actual
            int finalI = i;
            List<TermWithValues> documentTerms = termsList.stream()
                    .filter(term -> term.getDocId() == finalI).sorted(new TermComparator()).collect(Collectors.toList());

            // Ordena los términos por tf x idflog10 utilizando el comparador

            System.out.println("Document ID: " + i);
            writer.println("Document ID: " + i);

            // Imprime y escribe en el archivo los top n términos y sus valores
            for (int j = 0; j < top && j < documentTerms.size(); j++) {
                TermWithValues term = documentTerms.get(j);
                System.out.printf("%14s\t\ttf: %3d\t\tdf: %3d\t\tidf: %3.6f\t\ttf * idflog10: %10.5f\n",
                        term.getTerm(), term.getTf(), term.getDf(), term.getIdf(), term.getTfidf());
                writer.printf("%14s\t\ttf: %3d\t\tdf: %3d\t\tidf: %3.6f\t\ttf * idflog10: %10.5f\n",
                        term.getTerm(), term.getTf(), term.getDf(), term.getIdf(), term.getTfidf());
            }
            System.out.println();
            writer.println();
        }

        writer.close();


    }
}
