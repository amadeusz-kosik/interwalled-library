package me.kosik.interwalled.ailist

final case class Configuration(
    intervalsCountToCheckLookahead: Int,
    intervalsCountToTriggerExtraction: Int,
    maximumComponentSize: Int
)

object Configuration {
    def apply(): Configuration =
        Configuration(24, 16, 100000)
}

