# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.


<#
.SYNOPSIS
Retrieves information about PRunSrv on the local machine to start Neo4j programs

.DESCRIPTION
Retrieves information about PRunSrv (Apache Commons Daemon) on the local machine to start Neo4j services and utilities, tailored to the type of Neo4j edition

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER ForServerInstall
Retrieve the PrunSrv command line to install a Neo4j Server

.PARAMETER ForServerUninstall
Retrieve the PrunSrv command line to uninstall a Neo4j Server

.PARAMETER ForServerUpdate
Retrieve the PrunSrv command line to update a Neo4j Server

.PARAMETER ForConsole
Retrieve the PrunSrv command line to start a Neo4j Server in the console.

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
function Get-Neo4jPrunsrv
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low',DefaultParameterSetName = 'ConsoleInvoke')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false)]
    [pscustomobject]$Neo4jServer

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerInstallInvoke')]
    [switch]$ForServerInstall

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerUninstallInvoke')]
    [switch]$ForServerUninstall

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerUpdateInvoke')]
    [switch]$ForServerUpdate

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerStartInvoke')]
    [switch]$ForServerStart

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerStopInvoke')]
    [switch]$ForServerStop

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ConsoleInvoke')]
    [switch]$ForConsole
  )

  begin
  {
  }

  process
  {
    $JavaCMD = Get-Java -Neo4jServer $Neo4jServer -ForServer -ErrorAction Stop
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return 255
    }

    # JVMDLL is in %JAVA_HOME%\bin\server\jvm.dll
    $JvmDLL = Join-Path -Path (Join-Path -Path (Split-Path $JavaCMD.java -Parent) -ChildPath 'server') -ChildPath 'jvm.dll'
    if (-not (Test-Path -Path $JvmDLL)) { throw "Could not locate JVM.DLL at $JvmDLL" }

    # Get the Service Name
    $Name = Get-Neo4jWindowsServiceName -Neo4jServer $Neo4jServer -ErrorAction Stop

    # Find PRUNSRV for this architecture
    # This check will return the OS architecture even when running a 32bit app on 64bit OS
    switch ((Get-WmiObject -Class Win32_Processor | Select-Object -First 1).Addresswidth) {
      32 { $PrunSrvName = 'prunsrv-i386.exe' } # 4 Bytes = 32bit
      64 { $PrunSrvName = 'prunsrv-amd64.exe' } # 8 Bytes = 64bit
      default { throw "Unable to determine the architecture of this operating system (Integer is $([IntPtr]::Size))" }
    }
    $PrunsrvCMD = Join-Path (Join-Path -Path (Join-Path -Path $Neo4jServer.Home -ChildPath 'bin') -ChildPath 'tools') -ChildPath $PrunSrvName
    if (-not (Test-Path -Path $PrunsrvCMD)) { throw "Could not find PRUNSRV at $PrunsrvCMD" }

    # Build the PRUNSRV command line
    switch ($PsCmdlet.ParameterSetName) {
      "ServerInstallInvoke" {
        $PrunArgs += @("`"//IS//$($Name)`"")
      }
      "ServerUpdateInvoke" {
        $PrunArgs += @("`"//US//$($Name)`"")
      }
      { @("ServerInstallInvoke","ServerUpdateInvoke") -contains $_ } {

        $JvmOptions = @()

        Write-Verbose "Reading JVM settings from configuration"
        # Try neo4j.conf first, but then fallback to neo4j-wrapper.conf for backwards compatibility reasons
        $setting = (Get-Neo4jSetting -ConfigurationFile 'neo4j.conf' -Name 'dbms.jvm.additional' -Neo4jServer $Neo4jServer)
        if ($setting -ne $null) {
          $JvmOptions = [array](Merge-Neo4jJavaSettings -Source $JvmOptions -Add $setting.value)
        } else {
          $setting = (Get-Neo4jSetting -ConfigurationFile 'neo4j-wrapper.conf' -Name 'dbms.jvm.additional' -Neo4jServer $Neo4jServer)
          if ($setting -ne $null) {
            $JvmOptions = [array](Merge-Neo4jJavaSettings -Source $JvmOptions -Add $setting.value)
          }
        }

        # Pass through appropriate args from Java invocation to Prunsrv
        # These options take priority over settings in the wrapper
        Write-Verbose "Reading JVM settings from console java invocation"
        $JvmOptions = [array](Merge-Neo4jJavaSettings -Source $JvmOptions -Add ($JavaCMD.args | Where-Object { $_ -match '(^-D|^-X)' }))

        $PrunArgs += @("`"--StartMode=jvm`"",
          "`"--StartMethod=start`"",
          "`"--StartPath=$($Neo4jServer.Home)`"",
          "`"--StartParams=--config-dir=$($Neo4jServer.ConfDir)`"",
          "`"++StartParams=--home-dir=$($Neo4jServer.Home)`"",
          "`"--StopMode=jvm`"",
          "`"--StopMethod=stop`"",
          "`"--StopPath=$($Neo4jServer.Home)`"",
          "`"--Description=Neo4j Graph Database - $($Neo4jServer.Home)`"",
          "`"--DisplayName=Neo4j Graph Database - $Name`"",
          "`"--Jvm=$($JvmDLL)`"",
          "`"--LogPath=$($Neo4jServer.LogDir)`"",
          "`"--StdOutput=$(Join-Path -Path $Neo4jServer.LogDir -ChildPath 'neo4j.log')`"",
          "`"--StdError=$(Join-Path -Path $Neo4jServer.LogDir -ChildPath 'service-error.log')`"",
          "`"--LogPrefix=neo4j-service`"",
          "`"--Classpath=lib/*;plugins/*`"",
          "`"--JvmOptions=$($JvmOptions -join ';')`"",
          "`"--Startup=auto`""
        )

        # Check if Java invocation includes Java memory sizing
        $JavaCMD.args | ForEach-Object -Process {
          if ($Matches -ne $null) { $Matches.Clear() }
          if ($_ -match '^-Xms([\d]+)m$') {
            $PrunArgs += "`"--JvmMs`""
            $PrunArgs += "`"$($matches[1])`""
            Write-Verbose "Use JVM Start Memory of $($matches[1]) MB"
          }
          if ($Matches -ne $null) { $Matches.Clear() }
          if ($_ -match '^-Xmx([\d]+)m$') {
            $PrunArgs += "`"--JvmMx`""
            $PrunArgs += "`"$($matches[1])`""

            Write-Verbose "Use JVM Max Memory of $($matches[1]) MB"
          }
        }

        if ($Neo4jServer.ServerType -eq 'Enterprise') { $serverMainClass = 'com.neo4j.server.enterprise.CommercialEntryPoint' }
        if ($Neo4jServer.ServerType -eq 'Community') { $serverMainClass = 'org.neo4j.server.CommunityEntryPoint' }
        if ($Neo4jServer.DatabaseMode.ToUpper() -eq 'ARBITER') { $serverMainClass = 'org.neo4j.server.enterprise.ArbiterEntryPoint' }
        if ($serverMainClass -eq '') { Write-Error "Unable to determine the Server Main Class from the server information"; return $null }
        $PrunArgs += @("`"--StopClass=$($serverMainClass)`"",
          "`"--StartClass=$($serverMainClass)`"")
      }
      "ServerUninstallInvoke" { $PrunArgs += @("`"//DS//$($Name)`"") }
      "ServerStartInvoke" { $PrunArgs += @("`"//ES//$($Name)`"") }
      "ServerStopInvoke" { $PrunArgs += @("`"//SS//$($Name)`"") }
      "ConsoleInvoke" { $PrunArgs += @("`"//TS//$($Name)`"") }
      default {
        throw "Unknown ParameterSetName $($PsCmdlet.ParameterSetName)"
        return $null
      }
    }

    Write-Output @{ 'cmd' = $PrunsrvCMD; 'args' = $PrunArgs }
  }

  end
  {
  }
}

# SIG # Begin signature block
# MIId1gYJKoZIhvcNAQcCoIIdxzCCHcMCAQExDzANBglghkgBZQMEAgEFADB5Bgor
# BgEEAYI3AgEEoGswaTA0BgorBgEEAYI3AgEeMCYCAwEAAAQQH8w7YFlLCE63JNLG
# KX7zUQIBAAIBAAIBAAIBAAIBADAxMA0GCWCGSAFlAwQCAQUABCAN6YKW4EaFaEU/
# wN7qSjJ7DztpqoIPr5pbBagjLYpJ9aCCGEYwggPFMIICraADAgECAgEAMA0GCSqG
# SIb3DQEBCwUAMIGDMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTETMBEG
# A1UEBxMKU2NvdHRzZGFsZTEaMBgGA1UEChMRR29EYWRkeS5jb20sIEluYy4xMTAv
# BgNVBAMTKEdvIERhZGR5IFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5IC0gRzIw
# HhcNMDkwOTAxMDAwMDAwWhcNMzcxMjMxMjM1OTU5WjCBgzELMAkGA1UEBhMCVVMx
# EDAOBgNVBAgTB0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUxGjAYBgNVBAoT
# EUdvRGFkZHkuY29tLCBJbmMuMTEwLwYDVQQDEyhHbyBEYWRkeSBSb290IENlcnRp
# ZmljYXRlIEF1dGhvcml0eSAtIEcyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
# CgKCAQEAv3FiCPH6WTT3G8kYo/eASVjpIoMTpsUgQwE7hPHmhUmfJ+r2hBtOoLTb
# cJjHMgGxBT4HTu70+k8vWTAi56sZVmvigAf88xZ1gDlRe+X5NbZ0TqmNghPktj+p
# A4P6or6KFWp/3gvDthkUBcrqw6gElDtGfDIN8wBmIsiNaW02jBEYt9OyHGC0OPoC
# jM7T3UYH3go+6118yHz7sCtTpJJiaVElBWEaRIGMLKlDliPfrDqBmg4pxRyp6V0e
# tp6eMAo5zvGIgPtLXcwy7IViQyU0AlYnAZG0O3AqP26x6JyIAX2f1PnbU21gnb8s
# 51iruF9G/M7EGwM8CetJMVxpRrPgRwIDAQABo0IwQDAPBgNVHRMBAf8EBTADAQH/
# MA4GA1UdDwEB/wQEAwIBBjAdBgNVHQ4EFgQUOpqFBxBnKLbv9r0FQW4gwZTaD94w
# DQYJKoZIhvcNAQELBQADggEBAJnbXXnV+ZdZZwNh8X47BjF1LaEgjk9lh7T3ppy8
# 2Okv0Nta7s90jHO0OELaBXv4AnW4/aWx1672194Ty1MQfopG0Zf6ty4rEauQsCeA
# +eifWuk3n6vk32yzhRedPdkkT3mRNdZfBOuAg6uaAi21EPTYkMcEc0DtciWgqZ/s
# nqtoEplXxo8SOgmkvUT9BhU3wZvkMqPtOOjYZPMsfhT8Auqfzf8HaBfbIpA4LXqN
# 0VTxaeNfM8p6PXsK48p/Xznl4nW6xXYYM84s8C9Mrfex585PqMSbSlQGxX991QgP
# 4hz+fhe4rF721BayQwkMTfana7SZhGXKeoji4kS+XPfqHPUwggTQMIIDuKADAgEC
# AgEHMA0GCSqGSIb3DQEBCwUAMIGDMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJp
# em9uYTETMBEGA1UEBxMKU2NvdHRzZGFsZTEaMBgGA1UEChMRR29EYWRkeS5jb20s
# IEluYy4xMTAvBgNVBAMTKEdvIERhZGR5IFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9y
# aXR5IC0gRzIwHhcNMTEwNTAzMDcwMDAwWhcNMzEwNTAzMDcwMDAwWjCBtDELMAkG
# A1UEBhMCVVMxEDAOBgNVBAgTB0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUx
# GjAYBgNVBAoTEUdvRGFkZHkuY29tLCBJbmMuMS0wKwYDVQQLEyRodHRwOi8vY2Vy
# dHMuZ29kYWRkeS5jb20vcmVwb3NpdG9yeS8xMzAxBgNVBAMTKkdvIERhZGR5IFNl
# Y3VyZSBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBHMjCCASIwDQYJKoZIhvcNAQEB
# BQADggEPADCCAQoCggEBALngyxDUr3a91JNi6zBkuIEIbMME2WIXji//PmXPj85i
# 5jxSHNoWRUtVq3hrY4NikM4PaWyZyBoUi0zMRTPqiNyeo68r/oBhnXlXxM8u9D8w
# PF1H/JoWvMM3lkFRjhFLVPgovtCMvvAwOB7zsCb4Zkdjbd5xJkePOEdT0UYdtOPc
# AOpFrL28cdmqbwDb280wOnlPX0xH+B3vW8LEnWA7sbJDkdikM07qs9YnT60liqXG
# 9NXQpq50BWRXiLVEVdQtKjo++Li96TIKApRkxBY6UPFKrud5M68MIAd/6N8EOcJp
# AmxjUvp3wRvIdIfIuZMYUFQ1S2lOvDvTSS4f3MHSUvsCAwEAAaOCARowggEWMA8G
# A1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQWBBRAwr0njsw0
# gzCiM9f7bLPwtCyAzjAfBgNVHSMEGDAWgBQ6moUHEGcotu/2vQVBbiDBlNoP3jA0
# BggrBgEFBQcBAQQoMCYwJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmdvZGFkZHku
# Y29tLzA1BgNVHR8ELjAsMCqgKKAmhiRodHRwOi8vY3JsLmdvZGFkZHkuY29tL2dk
# cm9vdC1nMi5jcmwwRgYDVR0gBD8wPTA7BgRVHSAAMDMwMQYIKwYBBQUHAgEWJWh0
# dHBzOi8vY2VydHMuZ29kYWRkeS5jb20vcmVwb3NpdG9yeS8wDQYJKoZIhvcNAQEL
# BQADggEBAAh+bJMQyDi4lqmQS/+hX08E72w+nIgGyVCPpnP3VzEbvrzkL9v4utNb
# 4LTn5nliDgyi12pjczG19ahIpDsILaJdkNe0fCVPEVYwxLZEnXssneVe5u8MYaq/
# 5Cob7oSeuIN9wUPORKcTcA2RH/TIE62DYNnYcqhzJB61rCIOyheJYlhEG6uJJQEA
# D83EG2LbUbTTD1Eqm/S8c/x2zjakzdnYLOqum/UqspDRTXUYij+KQZAjfVtL/qQD
# WJtGssNgYIP4fVBBzsKhkMO77wIv0hVU7kQV2Qqup4oz7bEtdjYm3ATrn/dhHxXc
# h2/uRpYoraEmfQoJpy4Eo428+LwEMAEwggUAMIID6KADAgECAgEHMA0GCSqGSIb3
# DQEBCwUAMIGPMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTETMBEGA1UE
# BxMKU2NvdHRzZGFsZTElMCMGA1UEChMcU3RhcmZpZWxkIFRlY2hub2xvZ2llcywg
# SW5jLjEyMDAGA1UEAxMpU3RhcmZpZWxkIFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9y
# aXR5IC0gRzIwHhcNMTEwNTAzMDcwMDAwWhcNMzEwNTAzMDcwMDAwWjCBxjELMAkG
# A1UEBhMCVVMxEDAOBgNVBAgTB0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUx
# JTAjBgNVBAoTHFN0YXJmaWVsZCBUZWNobm9sb2dpZXMsIEluYy4xMzAxBgNVBAsT
# Kmh0dHA6Ly9jZXJ0cy5zdGFyZmllbGR0ZWNoLmNvbS9yZXBvc2l0b3J5LzE0MDIG
# A1UEAxMrU3RhcmZpZWxkIFNlY3VyZSBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBH
# MjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOWQZkvs+UZxqSCDvuls
# v0rJSGmBdU5tJPbLFxP4sHFZhHprK4WkNLUW5cvM6UFwLKQu1voyfeGo3pQQrDHB
# wNhq/1knq3bW/At0a7inrj/EVPS0MUTdk1aMpExem4nLJIOb4ld9t9gSH8mFbfTR
# gPFQm4eu1AsQBfsnuihtF+kO1k25OVUG/wokBX4vxh1ybNSLKYxXfdrZ62Ya00+n
# 339SxDDFpckOAsVTv3c4aAYkw2bIN34wHkVxIzX/kNgqnY3nsJJNPH8qCpPczRZG
# ZfdghIt2S5EncxSS4OrujxbqjQ4+dhe/fYmAgERD5y3gQwl12jborduJOvVdEo4j
# BIMCAwEAAaOCASwwggEoMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEG
# MB0GA1UdDgQWBBQlRYFoUCY4PTstLL7Natm2PbNmYzAfBgNVHSMEGDAWgBR8DDIf
# p9kwf8R9aKNiqKHOqwdbJzA6BggrBgEFBQcBAQQuMCwwKgYIKwYBBQUHMAGGHmh0
# dHA6Ly9vY3NwLnN0YXJmaWVsZHRlY2guY29tLzA7BgNVHR8ENDAyMDCgLqAshipo
# dHRwOi8vY3JsLnN0YXJmaWVsZHRlY2guY29tL3Nmcm9vdC1nMi5jcmwwTAYDVR0g
# BEUwQzBBBgRVHSAAMDkwNwYIKwYBBQUHAgEWK2h0dHBzOi8vY2VydHMuc3RhcmZp
# ZWxkdGVjaC5jb20vcmVwb3NpdG9yeS8wDQYJKoZIhvcNAQELBQADggEBAFZlyv7z
# Pwqok4sYx95DaRM0IL5OX3ioa5zbak1B28ET7NwxACJe9wCeDOA0ZTT5sTpOSMgS
# gYhcWz4IU3r3GmTfuFBhzFNRQClLwvSuOl/kyq0mzE5hQ+X9V6Y3cM5DK7CUw5Lp
# 4V+qEEm3aeTg0B9kpCvNH2+g+IQkGM55PamRv1QYE4mZVBENVcUmC3lPWhxu+WPb
# FICkB6v6sqW5iN2R/mU7pKN5volN4dCw9MgXDAqWFHwJt2zhwthV1BigqkFpcCSj
# ue/pWtw+65RK8LfeXw52+vv7aQNFQFDucgykEoaBzRPRTsQ8yk4N0ibxALe0pqLh
# bnqB/TCseh/HWXswggUhMIIECaADAgECAgkAhHYYKGL3whowDQYJKoZIhvcNAQEL
# BQAwgbQxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdBcml6b25hMRMwEQYDVQQHEwpT
# Y290dHNkYWxlMRowGAYDVQQKExFHb0RhZGR5LmNvbSwgSW5jLjEtMCsGA1UECxMk
# aHR0cDovL2NlcnRzLmdvZGFkZHkuY29tL3JlcG9zaXRvcnkvMTMwMQYDVQQDEypH
# byBEYWRkeSBTZWN1cmUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5IC0gRzIwHhcNMTcx
# MTA3MTkzNzAzWhcNMjAxMTA3MTkzNzAzWjBiMQswCQYDVQQGEwJVUzETMBEGA1UE
# CBMKQ2FsaWZvcm5pYTESMBAGA1UEBxMJU2FuIE1hdGVvMRQwEgYDVQQKEwtOZW80
# aiwgSW5jLjEUMBIGA1UEAxMLTmVvNGosIEluYy4wggEiMA0GCSqGSIb3DQEBAQUA
# A4IBDwAwggEKAoIBAQDSoPiG1pU1Lvqo+aZsFTrUwaV1sDWVBtfWzSnDKB3bUJeC
# 7DhekXtt1FORi3PB4YAC/CSMGgwoBHuqgGuRaJbHjRlmYaZZdKVsgvmDwfEvv16j
# zoyUR8TMTTjCemIDAHwArEadkffpsgnFpQ6KG6+gag/39FXyM2rGmFaqSGkqjVRN
# u4zN5GQu8+CUvRuZO2zEuKdA4wv9ZlmWbV3bpCGIN3Zl4p39Fatz3KYNi4g8lFXh
# B8tJfBToRuqxLZpcuyrXG3PeLa6DNoYOJ3j49DJOEw8Wj9cnqvAaI3CNE2klZ7RS
# cE47YUh7rVpl/ykp9ohgZDtvhAA5RYI5KCnc+oXHAgMBAAGjggGFMIIBgTAMBgNV
# HRMBAf8EAjAAMBMGA1UdJQQMMAoGCCsGAQUFBwMDMA4GA1UdDwEB/wQEAwIHgDA1
# BgNVHR8ELjAsMCqgKKAmhiRodHRwOi8vY3JsLmdvZGFkZHkuY29tL2dkaWcyczUt
# My5jcmwwXQYDVR0gBFYwVDBIBgtghkgBhv1tAQcXAjA5MDcGCCsGAQUFBwIBFito
# dHRwOi8vY2VydGlmaWNhdGVzLmdvZGFkZHkuY29tL3JlcG9zaXRvcnkvMAgGBmeB
# DAEEATB2BggrBgEFBQcBAQRqMGgwJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmdv
# ZGFkZHkuY29tLzBABggrBgEFBQcwAoY0aHR0cDovL2NlcnRpZmljYXRlcy5nb2Rh
# ZGR5LmNvbS9yZXBvc2l0b3J5L2dkaWcyLmNydDAfBgNVHSMEGDAWgBRAwr0njsw0
# gzCiM9f7bLPwtCyAzjAdBgNVHQ4EFgQUvj4gytCNJMDPx3lWv0klX6YK41IwDQYJ
# KoZIhvcNAQELBQADggEBABzaEnMJczETlZUdZE36x84eQS2AmumczZzTMbZ4IhJw
# xF8vVz2+Q+0BcR5uwAXa+s167yqIZsxAub3nu8GzYAF7D7wHDC1H1JNkgfnZf1w2
# WWGL6jkbr5RGrLlU2xE8o03iuFglU4QQl9ouXXBLAsLo/q+pMrPs+EO+g3DwXGFt
# jAKzkrMzJD5Ia2kVSC2aAXrffwRqMpbKVxkf0TQadMGLa6dVybYH7qBfDZ+u8P2K
# Y0qQyQYY63WoVk7TIq1VkbmRXtcvm3/plWPUNTPPEy0DfnjndA2UByib6/iqdnSZ
# 7MYit31rmSsRAS3Wil/qqOGlVfYrSm2s64ryPMOacAkwggV8MIIEZKADAgECAghX
# 62vcgKuJTDANBgkqhkiG9w0BAQsFADCBxjELMAkGA1UEBhMCVVMxEDAOBgNVBAgT
# B0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUxJTAjBgNVBAoTHFN0YXJmaWVs
# ZCBUZWNobm9sb2dpZXMsIEluYy4xMzAxBgNVBAsTKmh0dHA6Ly9jZXJ0cy5zdGFy
# ZmllbGR0ZWNoLmNvbS9yZXBvc2l0b3J5LzE0MDIGA1UEAxMrU3RhcmZpZWxkIFNl
# Y3VyZSBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBHMjAeFw0xOTA0MTUwNzAwMDBa
# Fw0yNDA0MTUwNzAwMDBaMIGHMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9u
# YTETMBEGA1UEBxMKU2NvdHRzZGFsZTEkMCIGA1UEChMbU3RhcmZpZWxkIFRlY2hu
# b2xvZ2llcywgTExDMSswKQYDVQQDEyJTdGFyZmllbGQgVGltZXN0YW1wIEF1dGhv
# cml0eSAtIEcyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzq8g420K
# 52oSzwX8q/EV5sVYhdhJzrc5/FQbiQOrax0PZYMoBKenJWo9sRQkwqkrxoTLzozz
# PqU/LG2H5knkw625cSMHB74FGHtPHjEju4tfnQFDI+XPzRGhvSPMrzPvg5zGGVT/
# Fku5z+AjU6LPPNFWR6vxVsz1Zq0y6cSLL0iJojP01SR6VtfNpzDMCu7p8OtpttH/
# GG/ohj3DMurv0NCrerzojzmWEr5oG8n2qtVAh2F/KmbzNmQ2wHNzGVFGm9YB1iVd
# G9M+V7zAs3HYbnSJKFi+k14UFgEYaiVnhxRCS+KREy8nEPaF6UII16Fxm3gJhJhO
# u3z25YPkk74PYQIDAQABo4IBqTCCAaUwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8E
# BAMCBsAwFgYDVR0lAQH/BAwwCgYIKwYBBQUHAwgwHQYDVR0OBBYEFHqrqS4tyGzj
# JXj2RtbFmwi6fyrvMB8GA1UdIwQYMBaAFCVFgWhQJjg9Oy0svs1q2bY9s2ZjMIGE
# BggrBgEFBQcBAQR4MHYwKgYIKwYBBQUHMAGGHmh0dHA6Ly9vY3NwLnN0YXJmaWVs
# ZHRlY2guY29tLzBIBggrBgEFBQcwAoY8aHR0cDovL2NybC5zdGFyZmllbGR0ZWNo
# LmNvbS9yZXBvc2l0b3J5L3NmX2lzc3VpbmdfY2EtZzIuY3J0MFQGA1UdHwRNMEsw
# SaBHoEWGQ2h0dHA6Ly9jcmwuc3RhcmZpZWxkdGVjaC5jb20vcmVwb3NpdG9yeS9t
# YXN0ZXJzdGFyZmllbGQyaXNzdWluZy5jcmwwUAYDVR0gBEkwRzBFBgtghkgBhv1u
# AQcXAjA2MDQGCCsGAQUFBwIBFihodHRwOi8vY3JsLnN0YXJmaWVsZHRlY2guY29t
# L3JlcG9zaXRvcnkvMA0GCSqGSIb3DQEBCwUAA4IBAQA4Pf90cACyj2vf+vYBwzlD
# 1DQg6ZZ8qQy4LvOSvlBz1mcTb0cR9FTUoyY40Y2KELo/VMY97rSUE3XeYiuCDdQS
# 9LCb2JlVF4m68aEFryeLqge7zvn039iYOw2icq5we8YuRd0JgCX338GSXPHHFdXX
# wz2pq/99uzoFczv86U3+usaG9tfYPg+hda1BIzCDPYS9YcqaqUgMAswM//VM4rX5
# TKfKiaWvfDSumRS4zAsQVHxeiov1ucfjnozIsPguFbO6wZDlfl/+kOMPnsmxD3HA
# cAb0LSVrgjdk6HR12SQKtOVhGtbXWQMUPpmK9Txqygmj17zMMdYlHwNwo9ZCUprM
# MYIE5jCCBOICAQEwgcIwgbQxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdBcml6b25h
# MRMwEQYDVQQHEwpTY290dHNkYWxlMRowGAYDVQQKExFHb0RhZGR5LmNvbSwgSW5j
# LjEtMCsGA1UECxMkaHR0cDovL2NlcnRzLmdvZGFkZHkuY29tL3JlcG9zaXRvcnkv
# MTMwMQYDVQQDEypHbyBEYWRkeSBTZWN1cmUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5
# IC0gRzICCQCEdhgoYvfCGjANBglghkgBZQMEAgEFAKCBhDAYBgorBgEEAYI3AgEM
# MQowCKACgAChAoAAMBkGCSqGSIb3DQEJAzEMBgorBgEEAYI3AgEEMBwGCisGAQQB
# gjcCAQsxDjAMBgorBgEEAYI3AgEVMC8GCSqGSIb3DQEJBDEiBCBmAYCmlgPAfzMZ
# Vr3VO5ClyvxcvlIgH4H5EbYy2LAqmDANBgkqhkiG9w0BAQEFAASCAQC0w8fzZ0Z0
# /kxBAr5u8Lp5anvOlZih07fYuDG5IYN/pJxImjMyASbUeG0WZMjpHDWromyy04PO
# 8L9enLjN7tXFF4iGPoHn3/3MnW8j455GsA55dAJq6+sq+zDrVuYvoHhvqWzbfcVQ
# +aAU7TvAPs6sfRFYam0dlNaJFHvJ95dO94GF/aKFW87tweA37/VagnDk13VcpyqL
# leWLvn7v76CNQQxF8dU5vNX5uYSH6xOa51s+yar8o3WZLHPM/jhlWonYkLWRO3DP
# NCpLkQxu/j0nrwVfSTBuZkgO+1IePWCA9PPZ53MFrxhvPAuzbfbcks+aPut/G5P1
# zdyz5G6zVou8oYICbTCCAmkGCSqGSIb3DQEJBjGCAlowggJWAgEBMIHTMIHGMQsw
# CQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTETMBEGA1UEBxMKU2NvdHRzZGFs
# ZTElMCMGA1UEChMcU3RhcmZpZWxkIFRlY2hub2xvZ2llcywgSW5jLjEzMDEGA1UE
# CxMqaHR0cDovL2NlcnRzLnN0YXJmaWVsZHRlY2guY29tL3JlcG9zaXRvcnkvMTQw
# MgYDVQQDEytTdGFyZmllbGQgU2VjdXJlIENlcnRpZmljYXRlIEF1dGhvcml0eSAt
# IEcyAghX62vcgKuJTDAJBgUrDgMCGgUAoF0wGAYJKoZIhvcNAQkDMQsGCSqGSIb3
# DQEHATAcBgkqhkiG9w0BCQUxDxcNMTkwNTIzMDY0MzM4WjAjBgkqhkiG9w0BCQQx
# FgQUnr9ERZybKidapF2XHII1YdQTDNgwDQYJKoZIhvcNAQEBBQAEggEADUMRzBpy
# BVxNe+AyF4xeogTZwfwLKC0Xch/0rJvNkG4d/sFhvnWPd1TvMuyf40EZzosPMnL+
# fmngcJ5TKE5z2M1yQ3Af5/wSZi/BQHAbcI69FvjsKqKckJFwmEl/5YWyy2NeNQ8q
# XYjDcvMX2IVdMqdgb7EGAU04X1OKq8Gf+gc1jNJJv09uCv/X4jiKKb3RJ+5y4Ga9
# GrM/b5eAXcl7ywP75EQsbQ3j8rG9UxNj4PTmd8ErOBZcOdX7RVDP0Rx+3KkZUjTC
# /hFi16zI5k+1fMuuildC9XIWsdruc7nLt52O/r0LkJa0xVVfsLH78RIF0M/6kBNl
# bwStEPwXLW4HRA==
# SIG # End signature block
