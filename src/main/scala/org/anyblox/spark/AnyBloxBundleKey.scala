package org.anyblox.spark

sealed trait AnyBloxBundleKey {
  def idString: String
}

case class AnyBloxExtensionBundleKey(anybloxPath: String, dataPath: String)
    extends AnyBloxBundleKey {
  override def idString: String = "anyblox-ext(" + anybloxPath + ", " + dataPath + ")"
}

case class AnyBloxSelfContainedBundleKey(path: String) extends AnyBloxBundleKey {
  override def idString: String = "anyblox-sc(" + path + ")"
}
