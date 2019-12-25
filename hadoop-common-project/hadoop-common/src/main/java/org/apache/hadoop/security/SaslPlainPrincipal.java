package org.apache.hadoop.security;

import java.security.Principal;

public class SaslPlainPrincipal implements Principal {
  private String userName;
  private char[] userPassword;

  public SaslPlainPrincipal(String userName, char[] userPassword) {
    this.userName = userName;
    this.userPassword = userPassword;
  }

  @Override
  public String getName() {
    return userName;
  }

  public char[] getPassword() {
    return userPassword;
  }

}
