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

package recipes_service;

import java.util.List;
import java.util.Timer;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Hosts;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.Recipe;
import recipes_service.data.Recipes;
import recipes_service.data.RemoveOperation;
import recipes_service.tsae.data_structures.Log;
import recipes_service.tsae.data_structures.Timestamp;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import recipes_service.tsae.sessions.TSAESessionOriginatorSide;
/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class ServerData {
	
	// server id
	private String id;
	
	// sequence number of the last recipe timestamped by this server
	private AtomicLong seqnum = new AtomicLong(Timestamp.NULL_TIMESTAMP_SEQ_NUMBER);


	// timestamp lock
	//private Object timestampLock = new Object();
	
	// TSAE data structures
	private Log log = null;
	private TimestampVector summary = null;
	private TimestampMatrix ack = null;
	
	// recipes data structure
	private Recipes recipes = new Recipes();

	// number of TSAE sessions
	int numSes = 1; // number of different partners that a server will contact for a TSAE session each time that TSAE timer (each sessionPeriod seconds) expires

	// propDegree: (default value: 0) number of TSAE sessions done each time a new data is created
	int propDegree = 0;
	
	// Participating nodes
	private Hosts participants;

	// TSAE timers
	private long sessionDelay;
	private long sessionPeriod = 10;

	private Timer tsaeSessionTimer;
	//
	TSAESessionOriginatorSide tsae = null;

	// TODO: esborrar aquesta estructura de dades
	// tombstones: timestamp of removed operations
	//List<Timestamp> tombstones = new Vector<Timestamp>();
	private List<Timestamp> tombstones = new CopyOnWriteArrayList<>();

//	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	// end: true when program should end; false otherwise
	private boolean end;

	public ServerData(){
	}

	private int currentSessionNumber = -1;

	public synchronized int getCurrentSessionNumber() {
		return this.currentSessionNumber;
	}

	public synchronized void setCurrentSessionNumber(int currentSessionNumber) {
		this.currentSessionNumber = currentSessionNumber;
	}

	/**
	 * Starts the execution
	 * @param participants
	 */
	public void startTSAE(Hosts participants){
		this.participants = participants;
		this.log = new Log(participants.getIds());
		this.summary = new TimestampVector(participants.getIds());
		this.ack = new TimestampMatrix(participants.getIds());
		

		//  Sets the Timer for TSAE sessions
	    tsae = new TSAESessionOriginatorSide(this);
		tsaeSessionTimer = new Timer();
		tsaeSessionTimer.scheduleAtFixedRate(tsae, sessionDelay, sessionPeriod);
	}

	public void stopTSAEsessions(){
		if (tsaeSessionTimer != null) {
			tsaeSessionTimer.cancel();
		}
	}
	
	public boolean end(){
		return this.end;
	}
	
	public void setEnd(){
		this.end = true;
	}

	// ******************************
	// *** timestamps
	// ******************************
	private Timestamp nextTimestamp(){
		// Iniciar secuencia
		if (seqnum.get() == Timestamp.NULL_TIMESTAMP_SEQ_NUMBER) {
			seqnum.set(-1);
		}
		// Generar nuevo timestamp con el ID del host y el incremento de la secuencia
		return  new Timestamp(id, seqnum.incrementAndGet());
	}

	// ******************************
	// *** add and remove recipes
	// ******************************
	public synchronized void addRecipe(String recipeTitle, String recipe) {

		if (recipeTitle == null || recipe == null) {
			LSimLogger.log(Level.WARN, "Invalid recipe input: title or content is null.");

			return;
		}

		Timestamp timestamp = nextTimestamp();
		Recipe rcpe = new Recipe(recipeTitle, recipe, id, timestamp);
		Operation addOps = new AddOperation(rcpe, timestamp);
		//invocamos función para realizar las actualizaciones y operaciones necesarias
		this.integrateOperation(addOps,false);

		LSimLogger.log(Level.TRACE, "[ServerData] [session: "+this.currentSessionNumber+" ] Recipe " + recipeTitle + " added to local storage and log.");

	}

	/**
	 * Elimina receta por título
	 * @param recipeTitle
	 */
	public synchronized void removeRecipe(String recipeTitle){
		Recipe removedRecipe = recipes.get(recipeTitle);
		if (removedRecipe != null) {
			Timestamp timestamp = nextTimestamp();
			RemoveOperation removeOps = new RemoveOperation(recipeTitle, removedRecipe.getTimestamp(), timestamp);
			// Esto sirve para recordar que la borramos y evitar que otro servidor nos la devuelva por error.
			tombstones.add(removedRecipe.getTimestamp());
			//invocamos función para realizar las actualizaciones y operaciones necesarias
			this.integrateOperation(removeOps, false);
			LSimLogger.log(Level.TRACE, "[ServerData] [session: "+this.currentSessionNumber+"  ] removeRecipe method: " + recipeTitle);
		} else {
			LSimLogger.log(Level.TRACE, "[ServerData] [session: "+this.currentSessionNumber+"  ] Try to remove null recipe: " + recipeTitle);
		}
	}

	public synchronized void purgeTombstones() {
		if (ack == null){
			return;
		}
		TimestampVector sum = ack.minTimestampVector();

		List<Timestamp> newTombstones = new Vector<Timestamp>();
		for(int i=0; i<tombstones.size(); i++){
			// Extraer el último tiempo confirmado para el host de este tombstone
			if (tombstones.get(i).compare(sum.getLast(tombstones.get(i).getHostid()))>0){
				// Si el tombstone es más nuevo que el tiempo confirmado, lo conservamos
				newTombstones.add(tombstones.get(i));
			}
		}
		// Actualizamos la referencia a la lista purgada
		tombstones = newTombstones;
	}


	// ****************************************************************************
	// *** operations to get the TSAE data structures. Used to send to evaluation
	// ****************************************************************************
	public Log getLog() {
		return log;
	}
	public TimestampVector getSummary() {
		return summary;
	}
	public TimestampMatrix getAck() {
		return ack;
	}
	public Recipes getRecipes(){
		return recipes;
	}

	// ******************************
	// *** getters and setters
	// ******************************
	public void setId(String id){
		this.id = id;		
	}
	public String getId(){
		return this.id;
	}

	public int getNumberSessions(){
		return numSes;
	}

	public void setNumberSessions(int numSes){
		this.numSes = numSes;
	}

	public int getPropagationDegree(){
		return this.propDegree;
	}

	public void setPropagationDegree(int propDegree){
		this.propDegree = propDegree;
	}

	public void setSessionDelay(long sessionDelay) {
		this.sessionDelay = sessionDelay;
	}
	public void setSessionPeriod(long sessionPeriod) {
		this.sessionPeriod = sessionPeriod;
	}
	public TSAESessionOriginatorSide getTSAESessionOriginatorSide(){
		return this.tsae;
	}
	
	// ******************************
	// *** other
	// ******************************
	
	public List<Host> getRandomPartners(int num){
		return participants.getRandomPartners(num);
	}
	
	/**
	 * waits until the Server is ready to receive TSAE sessions from partner servers   
	 */
	public synchronized void waitServerConnected(){
		while (!SimulationData.getInstance().isConnected()){
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//			e.printStackTrace();
			}
		}
	}
	
	/**
	 * 	Once the server is connected notifies to ServerPartnerSide that it is ready
	 *  to receive TSAE sessions from partner servers  
	 */ 
	public synchronized void notifyServerConnected(){
		notifyAll();
	}


	/**
	 * Nuevo meotdo centralizado: integraa de forma segura una operación
	 * recibida, actualizando BBDD, Log y Vector.
	 */

	public synchronized void integrateOperation(Operation op, boolean updateAck) {
		// Añadir al log de mensajes para propagación
		boolean addedToLog = log.add(op);

		Timestamp auxTimestamp = op.getTimestamp();

		// Si la operación es nueva (se añadió al log) la procesamos
		if (addedToLog) {
			// Actualizar vector local para reflejar que conocemos esta novedad
			summary.updateTimestamp(op.getTimestamp());
			// Actualizar ack con el sumary actual
			if (updateAck) {
				ack.update(id, summary);}
			synchronized (tombstones) {
				if (op instanceof AddOperation addOp) {
					if (tombstones.contains(addOp.getRecipe().getTimestamp())) {
						LSimLogger.log(Level.TRACE, "[ServerData] [session:  "+this.currentSessionNumber+" ] integrateOperation method: AddOperation ignored due to existing tombstone.");
					} else {
						Recipe recipeData = addOp.getRecipe();
						Recipe newRecipe = new Recipe(recipeData.getTitle(), recipeData.getRecipe(), recipeData.getAuthor(), recipeData.getTimestamp());
						// Agregar recepta
						recipes.add(newRecipe);
					}
				} else if (op instanceof RemoveOperation removeOp) {
					// Eliminar receta de nuestra lista local.
					recipes.remove(removeOp.getRecipeTitle());
					// Verificar si el tombstone contiene la marca de tiempo; en caso afirmativo, se elimina
					if (!tombstones.contains(removeOp.getRecipeTimestamp())) {
						tombstones.add(removeOp.getRecipeTimestamp());
					}
				}
			}
		}

		LSimLogger.log(Level.TRACE, "[ServerData] [session:  "+this.currentSessionNumber+" ] integrateOperation method: " + op.toString());

	}

}
