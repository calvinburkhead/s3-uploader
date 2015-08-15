package com.nwmsrocks.s3uploader;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.amazonaws.regions.Region;

public interface UploaderInterface extends Remote {

	public String getBucketName() throws RemoteException;

	public String getCredentialPath() throws RemoteException;

	public String getDestination() throws RemoteException;

	public int getLiveThreadCount() throws RemoteException;

	public int getMaximumThreadCount() throws RemoteException;

	public Region getRegion() throws RemoteException;

	public boolean isCreate() throws RemoteException;

	public boolean isDeleteAfterUpload() throws RemoteException;

	public boolean isPretend() throws RemoteException;

	public boolean isPurge() throws RemoteException;

	public boolean isRunning() throws RemoteException;

	public S3Object peek() throws RemoteException;

	public void queue(S3Object s3Object) throws RemoteException, InterruptedException;

	public void setBucketName(String bucketName) throws RemoteException;

	public void setCreate(boolean create) throws RemoteException;

	public void setCredentialPath(String credentialPath) throws RemoteException;

	public void setDeleteAfterUpload(boolean delete) throws RemoteException;

	public void setDestination(String prefix) throws RemoteException;

	public void setMaximumThreadCount(int maxThreads) throws RemoteException;

	public void setPretend(boolean pretend) throws RemoteException;

	public void setPurge(boolean purge) throws RemoteException;

	public void setRecurse(boolean recurse) throws RemoteException;

	public void setRegion(Region region) throws RemoteException;

	public void upload() throws RemoteException;
}
