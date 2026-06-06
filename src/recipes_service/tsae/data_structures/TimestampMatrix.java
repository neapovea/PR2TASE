/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){
			return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){
		// For each node in the matrix being merged
		for (String node : tsMatrix.timestampMatrix.keySet()) {
			// Get or create timestamp vector for this node
			TimestampVector currentVector = timestampMatrix.get(node);
			if (currentVector == null) {
				currentVector = new TimestampVector(new ArrayList<>(timestampMatrix.keySet()));
				timestampMatrix.put(node, currentVector);
			}

			// Update with max values from other matrix's vector
			currentVector.updateMax(tsMatrix.getTimestampVector(node));
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public synchronized void update(String node, TimestampVector tsVector){
		timestampMatrix.put(node, tsVector);

	}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public synchronized TimestampVector minTimestampVector(){

		// Si esta vacio devuelvo null
		if (timestampMatrix.isEmpty()) {
			return null;
		}

		// Crear vector con todos los datos
		TimestampVector minVector = new TimestampVector(new ArrayList<>(timestampMatrix.keySet()));

		boolean first = true;
		for (TimestampVector vector : timestampMatrix.values()) {
			if (first) {
				// La primera vez inicia vector
				for (String participant : vector.getTimestamps().keySet()) {
					minVector.update(participant, vector.getLast(participant));
				}
				first = false;
			} else {
				// Coger el valor mínimo
				minVector.mergeMin(vector);
			}
		}

		return minVector;
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampMatrix clone(){

		TimestampMatrix clonedMatrix = new TimestampMatrix(new ArrayList<>(timestampMatrix.keySet()));
		for (String node : timestampMatrix.keySet()) {
			clonedMatrix.timestampMatrix.put(node, timestampMatrix.get(node).clone());
		}
		return clonedMatrix;
	}
	
	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {

		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
		TimestampMatrix other = (TimestampMatrix) obj;
		return timestampMatrix.equals(other.timestampMatrix);
	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
