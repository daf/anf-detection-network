{
    "type":"application",
    "name":"anf",
    "description": "ANF Detection Network application",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'anf_app_controller_service',
        'anf.app_controller_service',
        'AppControllerService'], {}
    ),
    "registered": [
        "anf"
    ],
    "applications": [
        "ioncore", "attributestore"
    ]
}
