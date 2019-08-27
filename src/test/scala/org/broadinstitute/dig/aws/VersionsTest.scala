package org.broadinstitute.dig.aws

import org.scalatest.FunSuite
import java.time.Instant
import java.io.StringReader
import org.scalactic.source.Position.apply

/**
 * @author clint
 * Aug 1, 2018
 * 
 * Based on the VersionsTest class from LoamStream, created Oct 28, 2016.
 */
final class VersionsTest extends FunSuite {
  
  private val oct28th = Instant.parse("2016-10-28T18:52:40.889Z")
  private val oct29th = Instant.parse("2016-10-29T18:52:40.889Z")
  
  test("happy path") {
    //NB: Use versionInfo.properties from src/test/scala, which takes priority at test-time
    val versions = Versions.load().get
    
    assert(versions.name === "blerg")
    assert(versions.version === "zerg")
    assert(versions.branch === "glerg")
    assert(versions.lastCommit === Some("nerg"))
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === Some("flerg"))
    assert(versions.buildDate === oct28th)
    assert(versions.remoteUrl === Some("https://example.com/some-app"))
    
    val expected = s"blerg zerg (flerg) branch: glerg commit: nerg (PLUS uncommitted changes!) built on: $oct28th " + 
                    "from https://example.com/some-app"
    
    assert(versions.toString === expected)
  }
  
  test("happy path - uncommittedChanges is false") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=blerg
      uncommittedChanges=false
      describedVersion=nuh
      buildDate=2016-10-29T18:52:40.889Z
      remoteUrl=https://example.com/blah""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === Some("blerg"))
    assert(versions.anyUncommittedChanges === false)
    assert(versions.describedVersion === Some("nuh"))
    assert(versions.buildDate === oct29th)
    assert(versions.remoteUrl === Some("https://example.com/blah"))
    
    val expected = s"foo bar (nuh) branch: baz commit: blerg built on: $oct29th from https://example.com/blah"
    
    assert(versions.toString === expected)
  }
  
  test("happy path - no remoteUrl") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=blerg
      uncommittedChanges=false
      describedVersion=nuh
      buildDate=2016-10-29T18:52:40.889Z""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === Some("blerg"))
    assert(versions.anyUncommittedChanges === false)
    assert(versions.describedVersion === Some("nuh"))
    assert(versions.buildDate === oct29th)
    assert(versions.remoteUrl === None)
    
    val expected = s"foo bar (nuh) branch: baz commit: blerg built on: $oct29th from UNKNOWN origin"
    
    assert(versions.toString === expected)
  }
  
  test("no last commit") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=
      uncommittedChanges=true
      describedVersion=nuh
      buildDate=2016-10-28T18:52:40.889Z
      remoteUrl=https://example.com/blah""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === None)
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === Some("nuh"))
    assert(versions.buildDate === oct28th)
    assert(versions.remoteUrl === Some("https://example.com/blah"))
    
    val expected = s"foo bar (nuh) branch: baz commit: UNKNOWN (PLUS uncommitted changes!) built on: $oct28th " +
                    "from https://example.com/blah"
    
    assert(versions.toString === expected)
  }
  
  test("no described version") {
    val data = new StringReader(""".stripMargin
      name=foo
      version=bar
      branch=baz
      lastCommit=blerg
      uncommittedChanges=true
      describedVersion=
      buildDate=2016-10-28T18:52:40.889Z
      remoteUrl=https://example.com/blah""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === Some("blerg"))
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === None)
    assert(versions.buildDate === oct28th)
    assert(versions.remoteUrl === Some("https://example.com/blah"))
    
    val expected = s"foo bar (UNKNOWN) branch: baz commit: blerg (PLUS uncommitted changes!) built on: $oct28th " +
                    "from https://example.com/blah"
    
    assert(versions.toString === expected)
  }
  
  test("no optional fields") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=
      uncommittedChanges=true
      describedVersion=
      buildDate=2016-10-28T18:52:40.889Z""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === None)
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === None)
    assert(versions.buildDate === oct28th)
    assert(versions.remoteUrl === None)
    
    val expected = s"foo bar (UNKNOWN) branch: baz commit: UNKNOWN (PLUS uncommitted changes!) built on: $oct28th " +
                    "from UNKNOWN origin"
    
    assert(versions.toString === expected)
  }
  
  test("missing field") {
    val data = new StringReader("""
      name=foo
      sjadghasdjhasdg=bar
      branch=baz
      lastCommit=
      uncommittedChanges=true
      describedVersion=
      buildDate=2016-10-28T18:52:40.889Z
      remoteUrl=https://example.com/blah""")
    
    assert(Versions.loadFrom(data).isFailure)
  }
  
  test("junk input") {
    assert(Versions.loadFrom(new StringReader("kashdkjasdhkjasdh")).isFailure)
    assert(Versions.loadFrom(new StringReader("")).isFailure)
    assert(Versions.loadFrom(new StringReader("   ")).isFailure)
  }
}
