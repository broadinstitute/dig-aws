package org.broadinstitute.dig.aws.config

import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dig.aws.config.emr.SubnetId
import com.typesafe.config.Config
import org.broadinstitute.dig.aws.config.emr.ReleaseLabel
import org.broadinstitute.dig.aws.config.emr.SecurityGroupId
import org.broadinstitute.dig.aws.config.emr.RoleId

/**
 * @author clint
 * Sep 17, 2019
 */
object ValueReaders {
  implicit val subnetIdReader: ValueReader[SubnetId] = SimpleWrapperValueReader(SubnetId.apply)
  
  implicit val releaseLabelReader: ValueReader[ReleaseLabel] = SimpleWrapperValueReader(ReleaseLabel.apply)
  
  implicit val securityGroupIdReader: ValueReader[SecurityGroupId] = SimpleWrapperValueReader(SecurityGroupId.apply)
  
  implicit val roleIdReader: ValueReader[RoleId] = SimpleWrapperValueReader(RoleId.apply)
  
  private final case class SimpleWrapperValueReader[A](constructor: String => A) extends ValueReader[A] {
    override def read(config: Config, path: String): A = constructor(config.getString(path))
  }
}
