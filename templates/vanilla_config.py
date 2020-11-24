config = {

    'version': 1,

    'controllers': {

        "con1": {
            'type': 'opx1',
            'analog_outputs': {
                1: {'offset': +0.0},
            },
        }
    },

    'elements': {

        "qe1": {
            "singleInput": {
                "port": ("con1", 1)
            },
            'intermediate_frequency': 1e6,
            'operations': {
                'playOp': "constPulse",
            },
        },
    },

    "pulses": {
        "constPulse": {
            'operation': 'control',
            'length': 1000,
            'waveforms': {
                'single': 'const_wf'
            }
        },
    },

    "waveforms": {
        'const_wf': {
            'type': 'constant',
            'sample': 0.2
        },
    },
}
