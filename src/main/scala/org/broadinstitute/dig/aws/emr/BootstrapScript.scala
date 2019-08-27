package org.broadinstitute.dig.aws.emr

import java.net.URI

import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig

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
    new ScriptBootstrapActionConfig().withPath(uri.toString)
  }

  /** Create the configuration for the action to be used in cluster creation. */
  def config: BootstrapActionConfig = {
    new BootstrapActionConfig()
      .withScriptBootstrapAction(action)
      .withName(uri.basename)
  }
}

/** A MasterBootstrapScript is a script that is run only on the master node. */
class MasterBootstrapScript(uri: URI) extends BootstrapScript(uri) {

  /** Use the run-if script to test whether or not this is the master node.
    *
    * See: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html
    */
  override protected def action: ScriptBootstrapActionConfig = {
    new ScriptBootstrapActionConfig()
      .withPath("s3://elasticmapreduce/bootstrap-actions/run-if")
      .withArgs("instance.isMaster=true", uri.toString)
  }
}

/** A SlaveBootstrapScript is a script that is only run on a slave node. */
class SlaveBootstrapScript(uri: URI) extends BootstrapScript(uri) {

  /** Use the run-if script to test whether or not this is NOT the master node.
    *
    * See: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html
    */
  override protected def action: ScriptBootstrapActionConfig = {
    new ScriptBootstrapActionConfig()
      .withPath("s3://elasticmapreduce/bootstrap-actions/run-if")
      .withArgs("instance.isMaster=false", uri.toString)
  }
}
