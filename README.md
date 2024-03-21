# NodeJS Connect Voice Mail Kinesis Video Streams Handler Upgrade

This little repo was an experiment in taking an [existing solution that AWS provides](https://github.com/amazon-connect/amazon-connect-salesforce-scv/tree/master/Solutions/VMX2-VoicemailExpress) that currently uses the old AWS Javascript SDK V2 into using the V3 version, along with all the changes this brought.

An accompanying blogpost exists exploring some of the thoughts and other details, but do ensure you go check out the whole `VMX2-VoicemailExpress` solution, as rather than requiring Java using a NodeJS may be more approachable depending on your background.

NOTE: The modified version also includes a few other alterations, like processing the audio via muLaw format, which allows it to be used as an audio source within Connect. This is left as an interesting aside, however is not really relevant to the overall upgrade as most changes relate to the KVS processing prior to this.
