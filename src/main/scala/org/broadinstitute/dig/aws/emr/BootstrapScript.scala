package org.broadinstitute.dig.aws.emr

import java.net.URI

import software.amazon.awssdk.services.emr.model.BootstrapActionConfig
import software.amazon.awssdk.services.emr.model.ScriptBootstrapActionConfig

import org.broadinstitute.dig.aws.Implicits

/** A bootstrap script is a script in S3 that can be run by the cluster during
  * initialization (used to install software, create directories, etc).
  *
  * Optionally, it's allowed for the script to only run on the master node.
  */
class BootstrapScript(uri: URI) {
  import Implicits.RichURI

  /** Create a simple action configuration for this boostrap action. */
  protected def action: ScriptBootstrapActionConfig = {
    ScriptBootstrapActionConfig.builder.path(uri.toString).build
  }

  /** Create the configuration for the action to be used in cluster creation. */
  def config: BootstrapActionConfig = {
    BootstrapActionConfig.builder
      .scriptBootstrapAction(action)
      .name(uri.basename)
      .build
  }
}

/** A MasterBootstrapScript is a script that is run only on the master node. */
class MasterBootstrapScript(uri: URI) extends BootstrapScript(uri) {

  /** Use the run-if script to test whether or not this is the master node.
    *
    * See: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html
    */
  override protected def action: ScriptBootstrapActionConfig = {
    ScriptBootstrapActionConfig.builder
      .path("s3://elasticmapreduce/bootstrap-actions/run-if")
      .args("instance.isMaster=true", uri.toString)
      .build
  }
}

/** A SlaveBootstrapScript is a script that is only run on a slave node. */
class SlaveBootstrapScript(uri: URI) extends BootstrapScript(uri) {

  /** Use the run-if script to test whether or not this is NOT the master node.
    *
    * See: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html
    */
  override protected def action: ScriptBootstrapActionConfig = {
    ScriptBootstrapActionConfig.builder
      .path("s3://elasticmapreduce/bootstrap-actions/run-if")
      .args("instance.isMaster=false", uri.toString)
      .build
  }
}
