package org.elasticsearch.index.query.image;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

import net.semanticmetadata.lire.imageanalysis.LireFeature;


/**
 * Copied from {@link MatchAllDocsQuery}, calculate score for all docs
 */
public class ImageQuery extends Query {

    private String luceneFieldName;
    private LireFeature lireFeature;
    private float boost=1.0f;  
    @SuppressWarnings("deprecation")
	public ImageQuery(String luceneFieldName, LireFeature lireFeature, float boost) {
        this.luceneFieldName = luceneFieldName;
        this.lireFeature = lireFeature;
        this.boost=boost;
    }
    
    private class ImageScorer extends AbstractImageScorer {

        private final DocIdSetIterator disi;
        
        ImageScorer(IndexReader reader, Weight w) {
        	
            super(w, luceneFieldName, lireFeature, reader,boost);
            this.disi =DocIdSetIterator.all(reader.maxDoc());
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

    private class ImageWeight extends Weight {
    	  
        public ImageWeight(IndexSearcher searcher) {
        	super(ImageQuery.this);     	
        }

        @Override
        public String toString() {
            return "weight(" + ImageQuery.this + ")";
        }

        @Override
        public float getValueForNormalization() {
           return 1f;
        }

        @Override
        public void normalize(float norm, float boost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            return new ImageScorer(context.reader(),this);
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
                    return Explanation.match(score, "ImageQuery, product of:",details); 
                }
            }

            return Explanation.noMatch("no matching term");
        }

		@Override
		public void extractTerms(Set<Term> terms) {
		}
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
        return new ImageWeight(searcher);
    }
    
    @Override
    public float getBoost() {
      return this.boost;
    }
    
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(luceneFieldName);
        buffer.append(",");
        buffer.append(lireFeature.getClass().getSimpleName());
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ImageQuery))
            return false;
        ImageQuery other = (ImageQuery) o;
        return (this.getBoost() == other.getBoost())
                && luceneFieldName.equals(other.luceneFieldName)
                && lireFeature.equals(other.lireFeature);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + luceneFieldName.hashCode();
        result = 31 * result + lireFeature.hashCode();
        result = Float.floatToIntBits(getBoost()) ^ result;
        return result;
    }
}
