package com.nwmsrocks.s3uploader;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface UploaderInterface extends Remote {

	public void queue(S3Object s3Object) throws RemoteException;
	public void execute() throws RemoteException;
	
}
