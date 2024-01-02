package es.udc.fi.ri.practicari;

import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;

public class RemoveDuplicates {
    public static void main(String[] args) throws Exception {
        String indexPath = null;
        String outPath = null;

        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " -index INDEX_PATH -out PATH\n\n";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-out":
                    outPath = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (indexPath == null || outPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        int numDocs = reader.numDocs();
        long numTerms = reader.getSumTotalTermFreq("contents");
        System.out.println("Original index at: " + indexPath);
        System.out.println("Number of documents in the original index: " + numDocs);
        System.out.println("Number of terms in the original index: " + numTerms);

        FSDirectory outDir = FSDirectory.open(Paths.get(outPath));
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);//Para sobreescribir fichero de salida
        IndexWriter writer = new IndexWriter(outDir, config);

        ArrayList<String> contentsList = new ArrayList<>();
        String contents;

        for (int i = 0; i < numDocs; i++) {
            Document doc = reader.document(i);
            IndexableField contentsField = doc.getField("contents");
            if (contentsField != null) {
                contents = doc.get("contents");
            } else {
                contents = doc.get("hash");
            }

            if (contentsList.contains(contents)) {
                continue;
            }

            contentsList.add(contents);

            writer.addDocument(doc);
        }

        reader.close();
        writer.close();

        reader = DirectoryReader.open(FSDirectory.open(Paths.get(outPath)));
        numDocs = reader.numDocs();
        numTerms = reader.getSumTotalTermFreq("contents");

        System.out.println("New index created at: " + outPath);
        System.out.println("Number of documents in the new index: " + numDocs);
        System.out.println("Number of unique terms in the new index: " + numTerms);

        reader.close();
    }
}