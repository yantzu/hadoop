package org.apache.hadoop.yarn.api.impl.pb.client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Private
public class SaslPlainInheritedContainerManagementProtocolPBClientImpl
    extends ContainerManagementProtocolPBClientImpl {
  public SaslPlainInheritedContainerManagementProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    super(clientVersion, addr, conf);
  }

  @Override 
  public StartContainersResponse startContainers(StartContainersRequest requests)
      throws YarnException, IOException {
    String envUser = SecurityUtil.getEnvHadoopUserName();
    String envPassword = SecurityUtil.getEnvHadoopUserPassword();
    for (StartContainerRequest startContainerRequest : requests.getStartContainerRequests()) {
      Map<String, String> env = new HashMap<String, String>();
      env.putAll(startContainerRequest.getContainerLaunchContext().getEnvironment());
      if (StringUtils.isNotEmpty(envUser)) {
        env.put("HADOOP_USER_NAME", envUser);
      }
      if (StringUtils.isNotEmpty(envPassword)) {
        env.put("HADOOP_USER_PASSWORD", envPassword);
      }
      startContainerRequest.getContainerLaunchContext().setEnvironment(env);
    }
    return super.startContainers(requests);
  }
}
