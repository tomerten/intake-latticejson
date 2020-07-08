import intake
from .intake_latticejson import Latticejson, RemoteLatticejson, RemoteLatticeSource
intake.container.register_container('lattice',RemoteLatticeSource)
