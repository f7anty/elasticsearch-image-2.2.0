package org.elasticsearch.index.query.image;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.common.lucene.search.Queries;

import net.semanticmetadata.lire.imageanalysis.LireFeature;

/**
 * Query by hash first and only calculate score for top n matches
 */
public class ImageHashLimitQuery extends Query {

    private String hashFieldName;
    private int[] hashes;
    private int maxResult;//limit query
    private String luceneFieldName;
    private LireFeature lireFeature;
    private float boost=1.0f; 
    
    public ImageHashLimitQuery(String hashFieldName, int[] hashes, int maxResult, String luceneFieldName, LireFeature lireFeature, float boost) {
        this.hashFieldName = hashFieldName;
        this.hashes = hashes;
        this.maxResult = maxResult;
        this.luceneFieldName = luceneFieldName;
        this.lireFeature = lireFeature;
        this.boost=boost;
    }

    final class ImageHashScorer extends AbstractImageScorer {
        private final DocIdSetIterator disi;
        ImageHashScorer(Weight weight, LeafReaderContext context) {
            super(weight, luceneFieldName, lireFeature, context.reader(),ImageHashLimitQuery.this.getBoost());
            this.disi =DocIdSetIterator.all(context.reader().maxDoc());
        }
        
        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          return disi.advance(target);
        }

        @Override
        public long cost() {
          return disi.cost();
        }
    }

    final class ImageHashLimitWeight extends Weight {

        
        public ImageHashLimitWeight(IndexSearcher searcher)
                throws IOException {
        	super(ImageHashLimitQuery.this);
        }

        @Override
        public String toString() { return "weight(" + ImageHashLimitQuery.this + ")"; }

        @Override
        public float getValueForNormalization() {
            return 1f;
        }

        @Override
        public void normalize(float queryNorm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
        	 return new ImageHashScorer(this, context);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            
            Scorer scorer = scorer(context);
            if (scorer != null) {
                int newDoc = scorer.advance(doc);
                if (newDoc == doc) {
                	
                    float score = scorer.score();                  		
                    Collection<Explanation> details=new ArrayList<Explanation>();
                    if (getBoost() != 1.0f) {
                    	details.add(Explanation.match(getBoost(),"boost"));
                        score = score / getBoost();
                    }
                    details.add(Explanation.match(score ,"image score (1/distance)"));
                    return Explanation.match(score, "ImageHashLimitQuery, product of:",details); 

                }
            }

            return Explanation.noMatch("no matching term");
        }

		@Override
		public void extractTerms(Set<Term> terms) {
		}

    }


    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(new SimpleSimilarity());

        BooleanQuery booleanQuery = new BooleanQuery();
        for (int h : hashes) {
            booleanQuery.add(new BooleanClause(new TermQuery(new Term(hashFieldName, Integer.toString(h))), BooleanClause.Occur.SHOULD));
        }
        TopDocs topDocs = indexSearcher.search(booleanQuery, maxResult);

        if (topDocs.scoreDocs.length == 0) {  // no result find
            return Queries.newMatchNoDocsQuery().createWeight(searcher,false);
        }

        BitSet bitSet = new BitSet(topDocs.scoreDocs.length);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            bitSet.set(scoreDoc.doc);
        }

        return new ImageHashLimitWeight(searcher);
    }

    @Override
    public float getBoost() {
      return this.boost;
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(hashFieldName);
        buffer.append(",");
        buffer.append(Arrays.toString(hashes));
        buffer.append(",");
        buffer.append(maxResult);
        buffer.append(",");
        buffer.append(luceneFieldName);
        buffer.append(",");
        buffer.append(lireFeature.getClass().getSimpleName());
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ImageHashLimitQuery))
            return false;

        ImageHashLimitQuery that = (ImageHashLimitQuery) o;

        if (maxResult != that.maxResult) return false;
        if (!hashFieldName.equals(that.hashFieldName)) return false;
        if (!Arrays.equals(hashes, that.hashes)) return false;
        if (!lireFeature.equals(that.lireFeature)) return false;
        if (!luceneFieldName.equals(that.luceneFieldName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + hashFieldName.hashCode();
        result = 31 * result + Arrays.hashCode(hashes);
        result = 31 * result + maxResult;
        result = 31 * result + luceneFieldName.hashCode();
        result = 31 * result + lireFeature.hashCode();
        return result;
    }


    final class SimpleSimilarity extends ClassicSimilarity {
        @Override
        public float tf(float freq) {
            return 1;
        }

        @Override
        public float idf(long docFreq, long numDocs) {
            return 1;
        }

        @Override
        public float coord(int overlap, int maxOverlap) {
            return 1;
        }

        @Override
        public float queryNorm(float sumOfSquaredWeights) {
            return 1;
        }

        @Override
        public float lengthNorm(FieldInvertState state) {
            return 1;
        }

        @Override
        public float sloppyFreq(int distance) {
            return 1;
        }
    }
}
