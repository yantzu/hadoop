package org.apache.hadoop.yarn.factories.impl.pb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@InterfaceAudience.Private
public class SaslPlainInheritedRpcClientFactoryPBImpl implements RpcClientFactory {

  private static final Log LOG = LogFactory.getLog(SaslPlainInheritedRpcClientFactoryPBImpl.class);

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.client";
  private static final String PB_IMPL_CLASS_PREFIX = "SaslPlainInherited";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBClientImpl";

  private static final SaslPlainInheritedRpcClientFactoryPBImpl self =
      new SaslPlainInheritedRpcClientFactoryPBImpl();
  private Configuration localConf = new Configuration();
  private ConcurrentMap<Class<?>, Constructor<?>> cache =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  private static final RpcClientFactoryPBImpl parent = RpcClientFactoryPBImpl.get();

  public static SaslPlainInheritedRpcClientFactoryPBImpl get() {
    return SaslPlainInheritedRpcClientFactoryPBImpl.self;
  }

  private SaslPlainInheritedRpcClientFactoryPBImpl() {
  }

  @Override 
  public Object getClient(Class<?> protocol, long clientVersion, InetSocketAddress addr,
      Configuration conf) {

    Constructor<?> constructor = cache.get(protocol);
    if (constructor == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = localConf.getClassByName(getPBImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        return parent.getClient(protocol, clientVersion, addr, conf);
      }
      
      try {
        constructor = pbClazz.getConstructor(Long.TYPE, InetSocketAddress.class, Configuration.class);
        constructor.setAccessible(true);
        cache.putIfAbsent(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnRuntimeException("Could not find constructor with params: " + Long.TYPE + ", " + InetSocketAddress.class + ", " + Configuration.class, e);
      }
    }
    try {
      Object retObject = constructor.newInstance(clientVersion, addr, conf);
      return retObject;
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (InstantiationException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override 
  public void stopClient(Object proxy) {
    parent.stopClient(proxy);
  }

  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = PB_IMPL_CLASS_PREFIX + srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }

  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }

  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }
}
