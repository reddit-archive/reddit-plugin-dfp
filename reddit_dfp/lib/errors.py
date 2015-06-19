def get_reason(webfault):
    return webfault.fault.detail.ApiExceptionFault.errors.reason
