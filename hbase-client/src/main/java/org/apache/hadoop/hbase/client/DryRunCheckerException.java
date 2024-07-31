package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class DryRunCheckerException extends DoNotRetryIOException {

    private static final long serialVersionUID = 6907047686199321701L;

    public DryRunCheckerException() {
        super();
    }

    public DryRunCheckerException(String s) {
        super(s);
    }

}