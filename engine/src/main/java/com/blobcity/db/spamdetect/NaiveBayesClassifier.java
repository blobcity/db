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

import java.io.*;
import java.util.*;

/**
 * @author sanketsarang
 */
public class NaiveBayesClassifier 
{	
	//all unique words that are contained in spam and ling messages will be stored in here.
	private ArrayList<Word> FeatureRepository;
	
	//the keywords-features that we want to look for in ham messages.
	private HashSet<Word> hamKeywords;
	
	//the keywords-features that we want to look for in spam messages.
	private HashSet<Word> spamKeywords;
	
	//P(spam)
	private double spam_probability;

	//P(ham)
	private double ling_probability;
	
	//total number of ling messages that we checked.
	private int number_of_ham_messages;
	
	//total number of ling messages that we checked.
	private int number_of_spam_messages;
	
	//total number of messages that we checked spam+ling.
	private int number_of_messages;
	
	public NaiveBayesClassifier()
	{
		this.hamKeywords = new HashSet<Word>();
		this.spamKeywords = new HashSet<Word>();
		this.FeatureRepository = new ArrayList<Word>();
		this.spam_probability=0.0;
		this.ling_probability=0.0;
		this.number_of_messages=0;
		this.number_of_ham_messages=0;
		this.number_of_spam_messages=0;
	}
	
	public boolean classify(String msg)
	{
		//logP(ham) + Sum[ logP(t_i | ham)^x_i ].
		double hamSum = 0.0;
				
		//logP(spam) + Sum[ logP(t_i | spam)^x_i ].
		double spamSum = 0.0;
				
		//the map will contain all the words that were found in the message with their frequencies. 
		HashMap<String , Integer> msgWords = new HashMap<String , Integer>();
		
		StringTokenizer tok = new StringTokenizer(msg);
		
		while(tok.hasMoreTokens())
		{
			String word = tok.nextToken();
			msgWords.put(word , msgWords.containsKey(word)?msgWords.get(word)+1:1);
		}
		
	
		
		
		for(int i=0; i<FeatureRepository.size(); i++)
		{
			Word w = FeatureRepository.get(i);
			
			int exists=msgWords.containsKey(w.getWord())?msgWords.get(w.getWord()):0;
			
			hamSum  =  hamSum + ( exists*Math.log(w.getHamProbability()) );
			spamSum = spamSum + ( exists*Math.log(w.getSpamProbability()) );
			
		}
		
		
		hamSum += Math.log(ling_probability);
		spamSum += Math.log(spam_probability);
		
		if(hamSum >= spamSum) return false;
		return true;
	}
	
	
	
	
	private void ComputeProbabilities()
	{
		//SUM( N_t,ham ).
		int N_ham = 0;
		
		//SUM( N_t,spam ).
		int N_spam = 0;
		
		for(int i = 0; i<FeatureRepository.size(); i++)
		{
			N_ham  += FeatureRepository.get(i).getHamFrequency();
			N_spam +=  FeatureRepository.get(i).getSpamFrequency();
		}
		
		
		for(int i = 0; i<FeatureRepository.size(); i++)
		{
			FeatureRepository.get(i).computeHamProbability(FeatureRepository.size() , N_ham);
			FeatureRepository.get(i).computeSpamProbability(FeatureRepository.size() , N_spam);
		}
	}
	
	
	
	
	
	
	//---------------TRAINING METHODS USING AS FEATURES ALL THE WORDS THAT WERE FOUND IN MESSAGES---------------//
	
	
	public void train(String lingFilesPath , String spamFilesPath)
	{
		trainLing(lingFilesPath);
		trainSpam(spamFilesPath);
		number_of_messages=number_of_ham_messages+number_of_spam_messages;
		ling_probability=(double)number_of_ham_messages/(double)number_of_messages;
		spam_probability=(double)number_of_spam_messages/(double)number_of_messages;
		this.ComputeProbabilities();
	}
	
	
	
	
	private void trainLing(String lingFilesPath)
	{
		try
		{
			File directory = new File(lingFilesPath);
			File[] files = directory.listFiles();
			
			number_of_ham_messages=files.length;
			
			for(File f :files)
			{
				BufferedReader br = new BufferedReader(new FileReader(f));
				
				String line = br.readLine();
			
				while(line!=null)
				{
					StringTokenizer tok = new StringTokenizer(line);
				
					while(tok.hasMoreTokens())
					{
						Word w = new Word(tok.nextToken());
											
						int pos = FeatureRepository.indexOf(w);
					
						if(pos==-1) 
						{
							w.FoundInHam();
							FeatureRepository.add(w);
						}
						
						else FeatureRepository.get(pos).FoundInHam();
					}
					
					line=br.readLine();
				}
				
				br.close();
			}
		}
		
		catch(Exception e)
		{
			System.err.println("ERROR : "+e.getMessage());
		}
	}
	
	
	
	
	private void trainSpam(String spamFilesPath)
	{
		try
		{
			
			File directory = new File(spamFilesPath);
			File[] files = directory.listFiles();
			
			number_of_spam_messages=files.length;
			
			for(File f :files)
			{
				BufferedReader br = new BufferedReader(new FileReader(f));
				
				String line = br.readLine();
			
				while(line!=null)
				{
					StringTokenizer tok = new StringTokenizer(line);
				
					while(tok.hasMoreTokens())
					{
						Word w = new Word(tok.nextToken());
												
						int pos = FeatureRepository.indexOf(w);
					
						if(pos==-1) 
						{
							w.FoundInSpam();
							FeatureRepository.add(w);
						}
						
						else FeatureRepository.get(pos).FoundInSpam();
					}
						
					line=br.readLine();
				}
				
				br.close();
			}
		}
		
		catch(Exception e)
		{
			System.err.println("ERROR : "+e.getMessage());
		}
	}
	
	
	
	
	
	
	//---------------TRAINING METHODS USING AS FEATURES THE WORDS THAT WE HAVE CHOOSEN IN FEATURE SELECTION---------------//
	
	
	public void train(String lingFilesPath , String spamFilesPath , String hamFeaturesFile , String spamFeaturesFile)
	{
		loadHamKeywords(hamFeaturesFile);
		loadSpamKeywords(spamFeaturesFile);
		trainHamWithKeywords(lingFilesPath);
		trainSpamWithKeywords(spamFilesPath);
		number_of_messages=number_of_ham_messages+number_of_spam_messages;
		ling_probability=(double)number_of_ham_messages/(double)number_of_messages;
		spam_probability=(double)number_of_spam_messages/(double)number_of_messages;
		this.ComputeProbabilities();
	}
	
	
	
	
	private void trainHamWithKeywords(String lingFilesPath)
	{
		try
		{			
			File directory = new File(lingFilesPath);
			File[] files = directory.listFiles();
			
			number_of_ham_messages=files.length;
			
			for(File f :files)
			{
				BufferedReader br = new BufferedReader(new FileReader(f));
				
				String line = br.readLine();
			
				while(line!=null)
				{
					StringTokenizer tok = new StringTokenizer(line);
				
					while(tok.hasMoreTokens())
					{
						Word w = new Word(tok.nextToken());
						
						int pos = FeatureRepository.indexOf(w);
					
						if(pos==-1) 
						{
							if(hamKeywords.contains(w))
							{
								w.FoundInHam();
								FeatureRepository.add(w);
							}
						}
						
						else FeatureRepository.get(pos).FoundInHam();
					}
					
					line=br.readLine();
				}
				
				br.close();
			}
		}
		
		catch(Exception e)
		{
			System.err.println("ERROR : "+e.getMessage());
		}
	}
	
	
	
	
	private void trainSpamWithKeywords(String spamFilesPath)
	{
		try
		{			
			File directory = new File(spamFilesPath);
			File[] files = directory.listFiles();
			
			number_of_spam_messages=files.length;
			
			for(File f :files)
			{
				BufferedReader br = new BufferedReader(new FileReader(f));
				
				String line = br.readLine();
			
				while(line!=null)
				{
					StringTokenizer tok = new StringTokenizer(line);
				
					while(tok.hasMoreTokens())
					{
						Word w = new Word(tok.nextToken());
						
						
						int pos = FeatureRepository.indexOf(w);
					
						if(pos==-1 ) 
						{
							if(spamKeywords.contains(w))
							{
								w.FoundInSpam();
								FeatureRepository.add(w);
							}
						}
						
						else FeatureRepository.get(pos).FoundInSpam();
					}
					
					line=br.readLine();
				}
				
				br.close();
			}
		}
		
		catch(Exception e)
		{
			System.err.println("ERROR : "+e.getMessage());
		}
	}
	
	
	
	
	private void loadHamKeywords(String hamFeaturesFile)
	{
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(new File(hamFeaturesFile)));
			
			String word = br.readLine();
			
			while(word != null)
			{
				if(!word.trim().equals("")) hamKeywords.add(new Word(word.trim()));
				word = br.readLine();
			}
			
			br.close();
		}
		
		catch(Exception e)
		{
			System.err.println("ERROR : "+e.getMessage());
		}
	}
	
	
	
	
	private void loadSpamKeywords(String spamFeaturesFile)
	{
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(new File(spamFeaturesFile)));
			
			String word = br.readLine();
			
			while(word != null)
			{
				if(!word.trim().equals("")) spamKeywords.add(new Word(word.trim()));
				word = br.readLine();
			}
			
			br.close();
		}
		
		catch(Exception e)
		{
			System.err.println("ERROR : "+e.getMessage());
		}
	}

	
	
	
}