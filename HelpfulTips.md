Little things that are hard to document

# Tips and Tricks #
## Output Keys ##
If you want to override the S3 output key from within your service, simply set the key property on the MetaFile object you return from the service's executeService() method. The default is to use the output file's MD5 hash as the object key.

Another way to set the output key has recently been added to the WorkRequest schema. If you specify an 

&lt;OutputKey&gt;

 element, you also specify the mime-type. You can specify many such keys, by design, one per mime-type. If more than one key with the same mime-type is defined, you'll get one or the other (undetermined). If the service generated multiple files with the same mime-type (such as several tiff files), the AbstractBaseService will insert a numeric sequence after the last '/' in the key, but before the final part of the key, so a key like "images/someoutput.tif" would be modified to be "images/0\_someoutput.tif, images/1\_someoutput.tif, ...".

## Service Instance Parameters ##
Normally a service instance is passed a set of params in user data. Lifeguard normally passes the following "AccessID SecretKey QueuePrefix ServiceName". If you want to add other static parameters, you can specify those in the PoolConfig. The 

&lt;AdditionalParams&gt;

 element lets you supply values to be appended to the user data. Then, on the service instance, they can be parsed out in a script, or your java service code. The config might look like this, if you wanted to pass a bucket name (say, for jar files it needs to pull before running the service).
```
	<ServicePool>
		<ServiceName>Transcode</ServiceName>
		<ServiceAMI>ami-21c62248</ServiceAMI>
		<AdditionalParams>dak.service.jars</AdditionalParams>
		<PoolStatusQueue>poolStatusTranscode</PoolStatusQueue>
		<ServiceWorkQueue>transcode-input</ServiceWorkQueue>
		<RampUpInterval>2</RampUpInterval>
		<RampUpDelay>240</RampUpDelay>
		<RampDownInterval>1</RampDownInterval>
		<RampDownDelay>480</RampDownDelay>
		<MinSize>0</MinSize>
		<MaxSize>10</MaxSize>
		<QueueSizeFactor>25</QueueSizeFactor>
		<FindExistingServers>false</FindExistingServers>
	</ServicePool>
```