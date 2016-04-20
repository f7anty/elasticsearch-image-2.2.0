package org.elasticsearch.index.query.image;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

import net.semanticmetadata.lire.imageanalysis.LireFeature;

/**
 * Copied from {@link TermQuery}, query by hash first and only calculate score for matching docs
 */
public class ImageHashQuery extends Query {
    
	private final Term term;

    private String luceneFieldName;
    private LireFeature lireFeature;
    private ImageScoreCache imageScoreCache;
    private float boost=1.0f; 
    
    @SuppressWarnings("deprecation")
	public ImageHashQuery(Term t, String luceneFieldName, LireFeature lireFeature, ImageScoreCache imageScoreCache, float boost) {
        this.term = t;
        this.luceneFieldName = luceneFieldName;
        this.lireFeature = lireFeature;
        this.imageScoreCache = imageScoreCache;
        this.boost=boost;
    }
    
    final class ImageHashScorer extends AbstractImageScorer {
        private final PostingsEnum docsEnum;
        private final IndexReader reader;

        ImageHashScorer(Weight weight,PostingsEnum docsEnum, IndexReader reader) {
            super(weight, luceneFieldName, lireFeature, reader, boost);
            this.docsEnum = docsEnum;
            this.reader = reader;
        }

        @Override
        public int docID() {
            return docsEnum.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return docsEnum.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return docsEnum.advance(target);
        }

        @Override
        public long cost() {
            return docsEnum.cost();
        }
        
        @Override
        public float score() throws IOException {
            assert docID() != NO_MORE_DOCS;
            int docId = docID();
            String cacheKey = reader.toString() + ":" + docId;
            if (imageScoreCache.getScore(cacheKey) != null) {
                return 0f;  // BooleanScorer will add all score together, return 0 for docs already processed
            }
            float score = super.score();
            imageScoreCache.setScore(cacheKey, score);
            return score;
        }
    }

    final class ImageHashWeight extends Weight {
        private final TermContext termStates;

        public ImageHashWeight(IndexSearcher searcher, TermContext termStates)
                throws IOException {
        	super(ImageHashQuery.this);
            assert termStates != null : "TermContext must not be null";
            this.termStates = termStates;
        }

        @Override
        public String toString() { return "weight(" + ImageHashQuery.this + ")"; }

        @Override
        public float getValueForNormalization() {
            return 1f;
        }

        @Override
        public void normalize(float norm, float boost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            assert termStates.topReaderContext == ReaderUtil.getTopLevelContext(context) : "The top-reader used to create Weight (" + termStates.topReaderContext + ") is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
            final TermsEnum termsEnum = getTermsEnum(context);
            if (termsEnum == null) {
                return null;
            }
            
            PostingsEnum docs =termsEnum.postings(null);
            assert docs != null;
            return new ImageHashScorer(this, docs, context.reader());
        }

        private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
            final TermState state = termStates.get(context.ord);
            if (state == null) { // term is not present in that reader
                assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
                return null;
            }

            final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
            termsEnum.seekExact(term.bytes(), state);
            return termsEnum;
        }

		private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
           
        	return reader.docFreq(term) == 0;
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
                  
                    return Explanation.match(score, "ImageHashQuery, product of:",details);
                }
            }

            return Explanation.noMatch("no matching term");
        }


		@Override
		public void extractTerms(Set<Term> terms) {

			terms.add(getTerm());
		}
    }


    public Term getTerm() {
        return term;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        final IndexReaderContext context = searcher.getTopReaderContext();
        final TermContext termState = TermContext.build(context, term);
        return new ImageHashWeight(searcher, termState);
    }
    
    @Override
    public float getBoost() {
      return this.boost;
    }
    
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        buffer.append(";");
        buffer.append(luceneFieldName);
        buffer.append(",");
        buffer.append(lireFeature.getClass().getSimpleName());
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ImageHashQuery))
            return false;
        ImageHashQuery other = (ImageHashQuery)o;
        return (this.getBoost() == other.getBoost())
                && this.term.equals(other.term)
                & this.luceneFieldName.equals(other.luceneFieldName)
                && this.lireFeature.equals(other.lireFeature);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + term.hashCode();
        result = 31 * result + luceneFieldName.hashCode();
        result = 31 * result + lireFeature.hashCode();
        result = Float.floatToIntBits(getBoost()) ^ result;
        return result;
    }
}
