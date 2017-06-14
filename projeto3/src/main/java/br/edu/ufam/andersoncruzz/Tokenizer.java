/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.edu.ufam.andersoncruzz;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Tokenizer so that we have a consistent definition of a "word".
 */
public class Tokenizer {
  private static final Pattern PATTERN = Pattern.compile("(^[^a-z]+|[^a-z]+$)");

  //Stopwords retirada de http://www.ranks.nl/stopwords Google History
  static String[] stopwords = {"a", "i", "a", "about", "an", "are", "as", "at", "be", "by", "com", "for", "from", 
		  "how","in","is","it","of","on","or","that","the","this","to","was","what","when","where","who","will","with", "the", "www"}; 
  
  public static List<String> tokenize(String input) {
    List<String> tokens = new ArrayList<>();
    StringTokenizer itr = new StringTokenizer(input);
    while (itr.hasMoreTokens()) {
      String w = PATTERN.matcher(itr.nextToken().toLowerCase()).replaceAll("");
      if (w.length() != 0 && !verifyIsStopword(w)) {
        tokens.add(w);
      }
    }

    return tokens;
  }
  
  public static boolean verifyIsStopword(String word){
	
	  for (String stopword : stopwords){
		  if (stopword.equals(word)) return true;
	  }

	return false;
  }
}
