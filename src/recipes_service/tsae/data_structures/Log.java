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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private final ConcurrentHashMap<String, CopyOnWriteArrayList<Operation>> log = new ConcurrentHashMap<>();
//	private final ReadWriteLock lock = new ReentrantReadWriteLock();


	public Log(List<String> participants){
		// create an empty log
		for (String participant : participants) {
			log.put(participant, new CopyOnWriteArrayList<>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */


	public synchronized boolean add(Operation op){
		// Recuperar el ID del host
		String hostId = op.getTimestamp().getHostid();
		// Recuperar el log del host
		// Obtener o inicializar la lista de operaciones para este host
		CopyOnWriteArrayList<Operation> operationsList = log.computeIfAbsent(hostId, key -> new CopyOnWriteArrayList<>());

		// Comparar si esta vacia o tiempo actual con la última entrada son iguales
		if (operationsList.isEmpty() || operationsList.get(operationsList.size() - 1).getTimestamp().compare(op.getTimestamp()) < 0) {
			operationsList.add(op);
			return true;
		}
		return false;
	}



	/**
	 * @param sum The sum of timestamps to compare against.
	 * @return A list of operations that are newer than the given sum of timestamps.
	 */
	public synchronized List<Operation> listNewer(TimestampVector partnerSummary) {
		List<Operation> newOperations = new ArrayList<>();
		// Iterar sobre las entradas del log (operaciones por host)
		for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
			String hostId = entry.getKey();
			CopyOnWriteArrayList<Operation> hostOperationsList = entry.getValue();

			// saltar operaciones vacias
			if (hostOperationsList.isEmpty())
				continue;

			Timestamp lastSeenByPartner = partnerSummary.getLast(hostId);
			// Filtrar y añadir solo las operaciones que el compañero no ha visto
			for (Operation op : hostOperationsList) {
				if (op.getTimestamp().compare(lastSeenByPartner) > 0) {
					newOperations.add(op);
				}
			}
		}

		return newOperations;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public synchronized void purgeLog(TimestampMatrix ack){

		if (ack == null) return;

		// Get minimum timestamp vector from ack matrix
		TimestampVector minTimestampVector = ack.minTimestampVector();
		if (minTimestampVector == null) return;

		// For each host's operation list in the log
		for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
			String hostId = entry.getKey();
			CopyOnWriteArrayList<Operation> operations = entry.getValue();

			// Remove operations that have been acknowledged by all participants
			operations.removeIf(op -> {
				String opHostId = op.getTimestamp().getHostid();
				Timestamp minAck = minTimestampVector.getLast(opHostId);
				return minAck != null && op.getTimestamp().compare(minAck) <= 0;
			});
		}

	}

	/**
	 * Checks if the log contains an operation with the given timestamp.
	 *
	 * @param timestamp the timestamp to check for.
	 * @return true if the log contains an operation with the given timestamp, false otherwise.
	 */
	public boolean contains(Timestamp timestamp) {
		if (timestamp == null) {
			return false; // Return false if timestamp is null
		}

		// Iterate over each host's operation list in the log
		for (CopyOnWriteArrayList<Operation> operations : log.values()) {
			// Check if any operation matches the given timestamp
			for (Operation operation : operations) {
				if (operation.getTimestamp().equals(timestamp)) {
					return true; // Return true if a match is found
				}
			}
		}
		return false; // Return false if no match is found
	}

	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		// Verificar de identidad y nulidad básica
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;

		Log other = (Log) obj;
		// Comparar el mapa log de la instancia actual con el de la other
		return this.log.equals(other.log);
	}



	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
			String name="";
			for(Enumeration<CopyOnWriteArrayList<Operation>> en = log.elements();
				en.hasMoreElements(); ){
				List<Operation> sublog=en.nextElement();
				for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
					name+=en2.next().toString()+"\n";
				}
			}

			return name;
	}


}
