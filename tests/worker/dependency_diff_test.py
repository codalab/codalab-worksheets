import random
import unittest

from worker import dependency_diff

DEPENDENCIES_A = [
    ("0xa886584d430b4f6281b113e884b73630", ""),
    ("0xcf73f32a168e4e93867be294d4951b9d", ""),
    ("0x85c0399a0e954177923d109264ce6a2c", ""),
    ("0x096bead4801c43bba66cb87e9a527505", ""),
    ("0x4ce150f294bf4dbf9ce1b584b771f7cc", ""),
    ("0x49f7da8a13b144358f03a11e311c73b3", ""),
    ("0x9bb9795589bd4e808befd29b476c6d97", ""),
    ("0x36f2a7a332ee4ad5a9d1a3f2d3f83b40", ""),
    ("0x7bd230c445bf46659d8e337f73ff1dd1", ""),
    ("0x6459c0cb1fe544aba4761ab6dadc5e61", ""),
    ("0x0e196c64bc234d4dbbb49661a31e7acc", ""),
    ("0xbd71d104192447c5a488aae15ac2b115", ""),
    ("0x914d116a654047288a73fd5440025a62", ""),
    ("0x7633f493884a4f03be1f164d803c8d82", ""),
    ("0x2d81ea68ff4247e9a8c3ddeeb619d35b", ""),
    ("0x7f8d51cbd7ac4fb980eb576c895dc6f9", ""),
    ("0xb0d05eb6a09b461098191b59bef7eaef", ""),
    ("0x09d499185bb14f32ab657060191e5384", ""),
    ("0xf14a6646d14c4dc883a26f21b5465ef6", ""),
    ("0x8297e4f2ca4a41b69c19e7a2a5b93643", ""),
    ("0xfa9c7e8b385c45dda02921e880526dcc", ""),
    ("0x29046d255c804327a9ab0d4ea5c31af1", ""),
    ("0xaddc11e07486438897c0a5598e96bf5e", ""),
    ("0x761c576fe7c045949979e67dfe66819f", ""),
    ("0x553b2cf6fbfe414bb25703e508d9cfae", ""),
    ("0x584495e9afa94fe28df47d3168b9185a", ""),
    ("0x430001b83804427d8243846b4f5ce9b3", ""),
    ("0xc6b6de176a7a4640bfdd3fc59765ecb6", ""),
    ("0xa38098f96aa0467982219d4a6a5547d7", ""),
    ("0xdbd354e91882439fb916ae2b950d8477", ""),
    ("0xd1500a37d0354e778b3733576ef11592", ""),
    ("0x5b448bddc8d845b1b26acd09faba16cf", ""),
    ("0x3cd7d6927cd84ad78b252104d112f9dd", ""),
    ("0xd370022a04c74dd891687a9dbbd9b640", ""),
    ("0xc2742da5dc334b6ca264f807473ffb61", ""),
    ("0x6e6b95aedd344e85824cabb1192a0255", ""),
    ("0x6b5cd5bd4c13435b9f603678cf9456cd", ""),
    ("0x177393366e51414e8e7b71e90a3fb0f7", "")
]

DEPENDENCIES_B = [
    ("0xf84385924a9b4c6bb9ae8edca22cfed1", ""),
    ("0xfb80b9075b294d698f0399326ce2b4bc", ""),
    ("0x5ccdeea754014675a9757f75ddb35935", ""),
    ("0x37075da812d14512a737ffffe9d3e171", ""),
    ("0x259f73aa7b6a4c7e8550a29b247cd595", ""),
    ("0xbe86639cd5b8481fa0e7ea001c080a86", ""),
    ("0x6c472e155263431e832b1927229d0874", ""),
    ("0x051daa9ba8fe46caa96c1978ee61e529", ""),
    ("0x6459c0cb1fe544aba4761ab6dadc5e61", ""),
    ("0x0e196c64bc234d4dbbb49661a31e7acc", ""),
    ("0xbd71d104192447c5a488aae15ac2b115", ""),
    ("0x914d116a654047288a73fd5440025a62", ""),
    ("0x7633f493884a4f03be1f164d803c8d82", ""),
    ("0x2d81ea68ff4247e9a8c3ddeeb619d35b", ""),
    ("0x7f8d51cbd7ac4fb980eb576c895dc6f9", ""),
    ("0xb0d05eb6a09b461098191b59bef7eaef", ""),
    ("0x09d499185bb14f32ab657060191e5384", ""),
    ("0xf14a6646d14c4dc883a26f21b5465ef6", ""),
    ("0x8297e4f2ca4a41b69c19e7a2a5b93643", ""),
    ("0xfa9c7e8b385c45dda02921e880526dcc", ""),
    ("0x29046d255c804327a9ab0d4ea5c31af1", ""),
    ("0xaddc11e07486438897c0a5598e96bf5e", ""),
    ("0x761c576fe7c045949979e67dfe66819f", ""),
    ("0x553b2cf6fbfe414bb25703e508d9cfae", ""),
    ("0x584495e9afa94fe28df47d3168b9185a", ""),
    ("0x430001b83804427d8243846b4f5ce9b3", ""),
    ("0xc6b6de176a7a4640bfdd3fc59765ecb6", ""),
    ("0xa38098f96aa0467982219d4a6a5547d7", ""),
    ("0xdbd354e91882439fb916ae2b950d8477", ""),
    ("0xd1500a37d0354e778b3733576ef11592", ""),
    ("0x5b448bddc8d845b1b26acd09faba16cf", ""),
    ("0x092d661643f7428493e2e5425448c33e", ""),
    ("0x20e23e406de3406486f87f46f16ad625", ""),
    ("0x757a971852ca44029bb22178582037db", ""),
    ("0xa770d0d408ad495a8282a494f49069cb", ""),
    ("0xe38117ababb5425485d3c49289ca877d", ""),
    ("0x2e5deee5dfff4518b29481f8dc77eeea", ""),
    ("0xdb4835596d2b4c5c859446c0071aa699", ""),
    ("0x84075f3dbffc42818cc8604c2ed27b76", ""),
    ("0xbf100db3b83f4615b025f84a4fb9184a", ""),
    ("0x0295054b79694a84a0aef2921048c268", ""),
    ("0xc1d2d83b094b405798c7b44e7eeee195", ""),
    ("0x7b647864de644546818ce12318c7003f", ""),
    ("0x9cee5657d09f43d19765fb6fb28f10dd", ""),
    ("0xab412abd0ae745038584f37689199ce7", ""),
    ("0x64a31ec83e0642cca858177495961b87", ""),
    ("0x9018a9cb193343d780d0255fbc798c41", ""),
    ("0xace120ed399345fdb54b16e5aab0f1e8", ""),
    ("0xb8955d33062743519fee78d6b5aeead3", ""),
    ("0xe41af4be4c24467fb724409a54c6fffb", ""),
    ("0x3e670394f4cb4ef4916cf361259fccab", "")
]


class DependencyDiffTest(unittest.TestCase):
    def test_hash(self):
        self.assertNotEqual(dependency_diff.hash_dependencies(DEPENDENCIES_A),
                            dependency_diff.hash_dependencies(DEPENDENCIES_B))

        # hash should be invariant to order
        rnd = random.Random(1212)  # use seed to ensure deterministic test
        shuffled = list(DEPENDENCIES_A)
        rnd.shuffle(DEPENDENCIES_A)
        self.assertEqual(dependency_diff.hash_dependencies(DEPENDENCIES_A),
                         dependency_diff.hash_dependencies(shuffled))

    def test_diff_patch(self):
        # Generate patch for B on A
        patch = dependency_diff.diff_dependencies(DEPENDENCIES_A, DEPENDENCIES_B)

        # Check that the diff is nontrivial for our test case
        self.assertGreater(len(patch['+']), 1)
        self.assertGreater(len(patch['-']), 1)

        # Check that applying the patch on A gives us B
        self.assertItemsEqual(DEPENDENCIES_B,
                              dependency_diff.patch_dependencies(DEPENDENCIES_A, patch))


