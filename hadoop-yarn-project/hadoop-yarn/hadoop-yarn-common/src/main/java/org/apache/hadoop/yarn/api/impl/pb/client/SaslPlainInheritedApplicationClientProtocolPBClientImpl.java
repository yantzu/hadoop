package org.apache.hadoop.yarn.api.impl.pb.client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Private
public class SaslPlainInheritedApplicationClientProtocolPBClientImpl
    extends ApplicationClientProtocolPBClientImpl {
  public SaslPlainInheritedApplicationClientProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    super(clientVersion, addr, conf);
  }

  @Override 
  public SubmitApplicationResponse submitApplication(SubmitApplicationRequest request)
      throws YarnException, IOException {
    String envUser = SecurityUtil.getEnvHadoopUserName();
    String envPassword = SecurityUtil.getEnvHadoopUserPassword();
    Map<String, String> env = new HashMap<String, String>();
    env.putAll(request.getApplicationSubmissionContext().getAMContainerSpec().getEnvironment());
    if (StringUtils.isNotEmpty(envUser)) {
      env.put("HADOOP_USER_NAME", envUser);
    }
    if (StringUtils.isNotEmpty(envPassword)) {
      env.put("HADOOP_USER_PASSWORD", envPassword);
    }
    request.getApplicationSubmissionContext().getAMContainerSpec().setEnvironment(env);
    
    return super.submitApplication(request);
  }
}
