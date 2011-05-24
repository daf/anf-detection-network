{
    "type":"application",
    "name":"anf_app_agent",
    "description": "ANF Detection Network worker agent",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'anf_app_agent',
        'anf.app_agent',
        'AppAgent'], {}
    ),
    "registered": [
        "anf_app_agent"
    ],
    "applications": [
        "ioncore", "anf"
    ]
}
