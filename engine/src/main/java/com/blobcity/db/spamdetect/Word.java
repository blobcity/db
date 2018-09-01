/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.db.spamdetect;


/**
 * Every instance of this class represents a single word found in a message.
 *
 * @author sanketsarang
 */
public class Word
{
	//The word itself.
	private String word;
	
	//The number of times the word was found in a collection of ham messages.
	private int inHams;
	
	//The number of times the word was found in a collection of spam messages.
	private int inSpams;
	
	//P(word|ham)
	private double hamProbability;
	
	//P(word|spam)
	private double spamProbability;
	
	
	
	
	public Word(String word)
	{
		this.word=word;
		this.inHams = 0;
		this.inSpams = 0;
		this.hamProbability=0.0;
		this.spamProbability=0.0;
	}
	
	
	
	
	public void setWord(String word)
	{
		this.word=word;
	}
	
	
	
	
	public String getWord()
	{
		return this.word;
	}
	
	
	
	
	public void FoundInHam()
	{
		this.inHams++;
	}
	
	
	
	
	public int getHamFrequency()
	{
		return this.inHams;
	}
	
	
	
	
	public void computeHamProbability(int number_of_keywords , int sum)
	{
		//P(t | ling) = [1 + N(t, ling)] / [m + N(ling)]
		this.hamProbability= ( (double)(1+this.inHams)/(double)(number_of_keywords+sum) );
	}
	
	
	
	
	public void setHamProbability(double prob)
	{
		this.hamProbability=prob;
	}
	
	
	
	
	public double getHamProbability()
	{
		return hamProbability;
	}
	
	
	
	
	public void FoundInSpam()
	{
		this.inSpams++;
	}
	
	
	
	
	public int getSpamFrequency()
	{
		return this.inSpams;
	}
	
	
	
	
	public void computeSpamProbability(int number_of_keywords , int sum)
	{
		//P(t | spam) = [1 + N(t, spam)] / [m + N(spam)]
		this.spamProbability= ( (double)(1+this.inSpams)/(double)(number_of_keywords+sum) );
	}
	
	
	
	
	public void setSpamProbability(double prob)
	{
		this.spamProbability=prob;
	}
	
	
	
	
	public double getSpamProbability()
	{
		return spamProbability;
	}
	
	
	
	
	@Override
	public String toString()
	{
		return ("Word : "+this.word+
				"\nNumber of times the word was found in ham messages : " +this.inHams+
				"\nNumber of times the word was found in spam messages : "+this.inSpams+
				"\nP("+this.word+"|spam) = "+this.spamProbability+
				"\nP("+this.word+"|ham) = "+this.hamProbability);
	}
	
	
	
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj.getClass()!=this.getClass()) return false;
		
		Word w = (Word)obj;
		
		if(this.word.equalsIgnoreCase(w.word)) return true;
		
		return false;
	}
	
	
	
	
	@Override
	public int hashCode()
	{
		return this.word.hashCode();
	}


}